from __future__ import annotations

import datetime as dt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# -----------------------------
# PAGE
# -----------------------------
st.set_page_config(page_title="Gestion de stock", layout="wide")
st.title("📦 Gestion de stock")

DEFAULT_SEUIL_GLOBAL = 3
EMPLACEMENTS = ["STOCK", "CHANTIER", "DEPOT"]
TYPES = ["ENTREE", "SORTIE"]

# -----------------------------
# DB
# -----------------------------
@st.cache_resource
def get_engine() -> Engine:
    db_url = str(st.secrets.get("DB_URL", "")).strip()
    if not db_url:
        st.error("DB_URL manquant. Streamlit → Settings → Secrets.")
        st.stop()

    return create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=5,
    )

ENGINE = get_engine()


def exec_sql(sql: str, params: dict | None = None) -> None:
    params = params or {}
    with ENGINE.begin() as conn:
        conn.execute(text(sql), params)
    st.cache_data.clear()


@st.cache_data(ttl=10)
def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    params = params or {}
    with ENGINE.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def init_db() -> None:
    # Articles
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS articles (
            article TEXT PRIMARY KEY,
            designation TEXT NOT NULL,
            stock INTEGER NOT NULL DEFAULT 0,
            garantie INTEGER NOT NULL DEFAULT 0,
            seuil_piece INTEGER NULL
        );
        """
    )

    # Mouvements (on garde le nom date_mvt pour éviter erreurs si tu avais déjà un ancien schéma)
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS mouvements (
            id BIGSERIAL PRIMARY KEY,
            date_mvt DATE NOT NULL,
            article TEXT NOT NULL,
            designation TEXT NOT NULL,
            type_mvt TEXT NOT NULL,
            emplacement TEXT NOT NULL,
            quantite INTEGER NOT NULL,
            commentaire TEXT NULL,
            adresse TEXT NULL
        );
        """
    )

    # Adresses
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS adresses (
            id BIGSERIAL PRIMARY KEY,
            nom TEXT UNIQUE NOT NULL
        );
        """
    )

    # Settings
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )

    # Colonnes si base ancienne
    exec_sql("ALTER TABLE articles ADD COLUMN IF NOT EXISTS seuil_piece INTEGER;")
    exec_sql("ALTER TABLE mouvements ADD COLUMN IF NOT EXISTS adresse TEXT;")
    exec_sql("ALTER TABLE mouvements ADD COLUMN IF NOT EXISTS commentaire TEXT;")
    exec_sql("ALTER TABLE mouvements ADD COLUMN IF NOT EXISTS date_mvt DATE;")

    # Index (rapidité)
    exec_sql("CREATE INDEX IF NOT EXISTS idx_articles_stock ON articles(stock);")
    exec_sql("CREATE INDEX IF NOT EXISTS idx_mouvements_article ON mouvements(article);")
    exec_sql("CREATE INDEX IF NOT EXISTS idx_mouvements_date ON mouvements(date_mvt);")

    # Seuil global par défaut
    df = read_df("SELECT value FROM settings WHERE key='seuil_global'")
    if df.empty:
        exec_sql(
            "INSERT INTO settings(key,value) VALUES('seuil_global', :v)",
            {"v": str(DEFAULT_SEUIL_GLOBAL)},
        )


def get_setting(key: str, default: str) -> str:
    df = read_df("SELECT value FROM settings WHERE key=:k", {"k": key})
    return default if df.empty else str(df.iloc[0]["value"])


def set_setting(key: str, value: str) -> None:
    exec_sql(
        """
        INSERT INTO settings(key,value)
        VALUES(:k,:v)
        ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value
        """,
        {"k": key, "v": str(value)},
    )


init_db()

# -----------------------------
# HELPERS
# -----------------------------
def upsert_article(article: str, designation: str, seuil_piece: int | None) -> None:
    exec_sql(
        """
        INSERT INTO articles(article, designation, seuil_piece)
        VALUES(:a,:d,:sp)
        ON CONFLICT(article) DO UPDATE
        SET designation = EXCLUDED.designation,
            seuil_piece = COALESCE(EXCLUDED.seuil_piece, articles.seuil_piece)
        """,
        {"a": article, "d": designation, "sp": seuil_piece},
    )


def set_seuil_piece(article: str, seuil_piece: int | None) -> None:
    exec_sql(
        "UPDATE articles SET seuil_piece=:sp WHERE article=:a",
        {"sp": seuil_piece, "a": article},
    )


def set_stock(article: str, new_stock: int) -> None:
    exec_sql(
        "UPDATE articles SET stock=:s WHERE article=:a",
        {"s": int(new_stock), "a": article},
    )


def apply_movement(article: str, qty: int, type_mvt: str) -> None:
    delta = int(qty) if type_mvt == "ENTREE" else -int(qty)
    exec_sql(
        "UPDATE articles SET stock = GREATEST(stock + :d, 0) WHERE article=:a",
        {"d": delta, "a": article},
    )


def add_mouvement(
    date_mvt: dt.date,
    article: str,
    designation: str,
    type_mvt: str,
    emplacement: str,
    quantite: int,
    commentaire: str | None,
    adresse: str | None,
) -> None:
    exec_sql(
        """
        INSERT INTO mouvements(date_mvt, article, designation, type_mvt, emplacement, quantite, commentaire, adresse)
        VALUES(:dt,:a,:d,:t,:e,:q,:c,:adr)
        """,
        {
            "dt": date_mvt,
            "a": article,
            "d": designation,
            "t": type_mvt,
            "e": emplacement,
            "q": int(quantite),
            "c": commentaire if commentaire else None,
            "adr": adresse if adresse else None,
        },
    )


# -----------------------------
# UI TABS
# -----------------------------
tab_mvt, tab_stock, tab_addr = st.tabs(["➕ Mouvement", "📦 Stock actuel", "📍 Adresses"])

# ==========================================================
# TAB 1 : MOUVEMENT
# ==========================================================
with tab_mvt:
    st.subheader("Ajouter un mouvement")

    # Charger adresses
    adf = read_df("SELECT nom FROM adresses ORDER BY nom")
    addr_list = adf["nom"].tolist()

    col1, col2, col3 = st.columns(3)

    with col1:
        date_mvt = st.date_input("Date", value=dt.date.today())
        article = st.text_input("Numéro d'article", placeholder="Ex: 155082").strip()

        designation_auto = ""
        seuil_auto = 0

        if article:
            ex = read_df(
                "SELECT designation, COALESCE(seuil_piece,0) AS sp FROM articles WHERE article=:a",
                {"a": article},
            )
            if not ex.empty:
                designation_auto = str(ex.iloc[0]["designation"])
                seuil_auto = int(ex.iloc[0]["sp"])

        designation = st.text_input("Désignation", value=designation_auto, placeholder="Ex: Sonde O2").strip()

        seuil_piece = st.number_input(
            "Seuil pièce (0 = aucun, sinon seuil personnalisé)",
            min_value=0,
            value=int(seuil_auto),
            step=1,
            help="Tu peux définir un seuil spécifique pour cette pièce.",
        )

        maj_seuil_existant = st.checkbox(
            "Mettre à jour le seuil de cette pièce (même si elle existe déjà)",
            value=True,
        )

    with col2:
        emplacement = st.selectbox("Emplacement", EMPLACEMENTS, index=0)
        type_mvt = st.selectbox("Type", TYPES, index=0)
        quantite = st.number_input("Quantité", min_value=1, value=1, step=1)

        adresse = st.selectbox("Adresse (optionnel)", [""] + addr_list, index=0)

    with col3:
        commentaire = st.text_area("Remarque / commentaire (optionnel)", height=120)
        if st.button("✅ Enregistrer", use_container_width=True):
            if not article:
                st.error("Numéro d'article obligatoire.")
                st.stop()
            if not designation:
                st.error("Désignation obligatoire.")
                st.stop()

            # Upsert article (désignation)
            sp_to_store = None if int(seuil_piece) == 0 else int(seuil_piece)
            upsert_article(article, designation, sp_to_store if maj_seuil_existant else None)

            # Stock
            apply_movement(article, int(quantite), type_mvt)

            # Historique
            add_mouvement(
                date_mvt=date_mvt,
                article=article,
                designation=designation,
                type_mvt=type_mvt,
                emplacement=emplacement,
                quantite=int(quantite),
                commentaire=commentaire.strip() if commentaire else None,
                adresse=adresse.strip() if adresse else None,
            )

            st.success("✅ Mouvement enregistré !")

    st.divider()
  

# ==========================================================
# TAB 2 : STOCK ACTUEL
# ==========================================================
with tab_stock:
    st.subheader("Stock actuel")

    search = st.text_input("Recherche", placeholder="Numéro ou mot dans désignation…").strip().lower()

    df = read_df(
        """
        SELECT article, designation, stock, garantie, COALESCE(seuil_piece,0) AS seuil_piece
        FROM articles
        ORDER BY article
        """
    )

    if search:
        df_view = df[
            df["article"].astype(str).str.lower().str.contains(search, na=False)
            | df["designation"].astype(str).str.lower().str.contains(search, na=False)
        ].copy()
    else:
        df_view = df.copy()

    st.dataframe(df_view, use_container_width=True, height=360)

    st.divider()

    st.subheader("Historique (300 derniers)")

    hist = read_df(
        """
        SELECT id, date_mvt, article, designation, type_mvt, emplacement, quantite,
               COALESCE(adresse,'') AS adresse,
               COALESCE(commentaire,'') AS commentaire
        FROM mouvements
        ORDER BY id DESC
        LIMIT 300
        """
    )
    
    st.dataframe(hist, use_container_width=True, height=420)
        # ------- SUPPRIMER -------
    st.subheader("🗑️ Supprimer un article (tout en bas)")

    st.warning("⚠️ Supprime aussi les mouvements liés à cet article.")
    articles_df = read_df("SELECT article FROM articles ORDER BY article")
articles_list = articles_df["article"].astype(str).tolist()
    
    if len(articles_list) >0:
        del_article = st.selectbox("Article à supprimer", articles_list, key="del_article")
        confirm = st.checkbox("Je confirme la suppression définitive", value=False)

        if st.button("❌ Supprimer définitivement", use_container_width=True, disabled=not confirm):
            exec_sql("DELETE FROM mouvements WHERE article=:a", {"a": del_article})
            exec_sql("DELETE FROM articles WHERE article=:a", {"a": del_article})
            st.success("✅ Article supprimé.")
    else:
        st.info("Aucun article à supprimer.")

    # ------- PIECES A COMMANDER -------
    st.subheader("📦 Pièces à commander")

    seuil_global = int(get_setting("seuil_global", str(DEFAULT_SEUIL_GLOBAL)))
    new_seuil_global = st.number_input(
        "Seuil global (utilisé si seuil pièce = 0)",
        min_value=0,
        value=int(seuil_global),
        step=1,
    )
    if int(new_seuil_global) != int(seuil_global):
        set_setting("seuil_global", str(int(new_seuil_global)))
        seuil_global = int(new_seuil_global)

    df_cmd = df.copy()
    df_cmd["seuil_utilise"] = df_cmd["seuil_piece"].apply(lambda sp: int(sp) if int(sp) > 0 else int(seuil_global))
    df_cmd = df_cmd[df_cmd["stock"] <= df_cmd["seuil_utilise"]].copy()
    df_cmd = df_cmd.sort_values(["stock", "designation", "article"])

    if df_cmd.empty:
        st.success("✅ Rien à commander")
    else:
        st.dataframe(
            df_cmd[["article", "designation", "stock", "seuil_piece", "seuil_utilise"]],
            use_container_width=True,
            height=320,
        )

    st.divider()

    # ------- MODIFIER ARTICLE -------
    st.subheader("✏️ Modifier un article (y compris seuil pièce)")

    articles_list = df["article"].tolist()
    if not articles_list:
        st.info("Aucun article.")
    else:
        a_sel = st.selectbox("Choisir un article", articles_list)

        row = read_df(
            """
            SELECT article, designation, stock, garantie, seuil_piece
            FROM articles
            WHERE article=:a
            """,
            {"a": a_sel},
        ).iloc[0]

        c1, c2 = st.columns(2)
        with c1:
            new_article = st.text_input("Numéro", value=str(row["article"])).strip()
            new_designation = st.text_input("Désignation", value=str(row["designation"])).strip()
            new_stock = st.number_input("Stock", min_value=0, value=int(row["stock"]), step=1)
        with c2:
            new_garantie = st.number_input("Garantie", min_value=0, value=int(row["garantie"]), step=1)
            sp_val = 0 if row["seuil_piece"] is None else int(row["seuil_piece"])
            new_sp = st.number_input("Seuil pièce (0 = aucun)", min_value=0, value=int(sp_val), step=1)

        update_hist = st.checkbox(
            "Mettre à jour aussi l'historique (mouvements) si le numéro change",
            value=True,
            help="Si tu changes le numéro d'article, on met à jour les mouvements aussi.",
        )

        if st.button("💾 Enregistrer modifications", use_container_width=True):
            if not new_article:
                st.error("Numéro vide.")
                st.stop()
            if not new_designation:
                st.error("Désignation vide.")
                st.stop()

            sp_to_store = None if int(new_sp) == 0 else int(new_sp)
            old_article = str(row["article"]).strip()

            # Update articles
            exec_sql(
                """
                UPDATE articles
                SET article=:newa,
                    designation=:d,
                    stock=:s,
                    garantie=:g,
                    seuil_piece=:sp
                WHERE article=:olda
                """,
                {
                    "newa": new_article,
                    "d": new_designation,
                    "s": int(new_stock),
                    "g": int(new_garantie),
                    "sp": sp_to_store,
                    "olda": old_article,
                },
            )

            # Update mouvements if requested
            if update_hist and new_article != old_article:
                exec_sql(
                    "UPDATE mouvements SET article=:newa WHERE article=:olda",
                    {"newa": new_article, "olda": old_article},
                )

            # Update designation in mouvements (optionnel mais logique)
            exec_sql(
                "UPDATE mouvements SET designation=:d WHERE article=:a",
                {"d": new_designation, "a": new_article},
            )

            st.success("✅ Article modifié ! (Recharge si besoin)")

    st.divider()


# ==========================================================
# TAB 3 : ADRESSES
# ==========================================================
with tab_addr:
    st.subheader("📍 Adresses")

    left, right = st.columns(2)

    with left:
        st.markdown("### Ajouter une adresse")
        new_addr = st.text_input("Nom", placeholder="Ex: Client Dupont / Chantier X").strip()
        if st.button("➕ Ajouter", use_container_width=True):
            if not new_addr:
                st.error("Nom vide.")
            else:
                exec_sql(
                    """
                    INSERT INTO adresses(nom)
                    VALUES(:n)
                    ON CONFLICT(nom) DO NOTHING
                    """,
                    {"n": new_addr},
                )
                st.success("✅ Adresse ajoutée.")

    with right:
        st.markdown("### Supprimer une adresse")
        adf2 = read_df("SELECT nom FROM adresses ORDER BY nom")
        alist = adf2["nom"].tolist()
        if alist:
            addr_del = st.selectbox("Adresse", alist)
            if st.button("🗑️ Supprimer", use_container_width=True):
                exec_sql("DELETE FROM adresses WHERE nom=:n", {"n": addr_del})
                st.success("✅ Adresse supprimée.")
        else:
            st.info("Aucune adresse enregistrée.")

    st.divider()
    st.markdown("### Liste")
    st.dataframe(read_df("SELECT nom FROM adresses ORDER BY nom"), use_container_width=True, height=380)



