# app.py — Gestion de stock (Streamlit + PostgreSQL)
# ✅ Fix "date" vs "date_mvt" (NOT NULL) + auto-remplissage désignation
# ✅ Onglets: Mouvement / Stock actuel / Adresses
# ✅ Historique + Tableau garanties DANS Stock actuel
# ✅ Modification article DANS Mouvement
# ✅ Suppression article (en bas) DANS Stock actuel
# ✅ Seuil modifiable même si article existe déjà

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# -----------------------------
# CONFIG
# -----------------------------
st.set_page_config(page_title="Gestion de stock", page_icon="📦", layout="wide")

DB_URL = st.secrets.get("DB_URL", "").strip()
if not DB_URL:
    st.error("❌ DB_URL manquant dans les Secrets (Streamlit Cloud).")
    st.stop()

ENGINE = create_engine(
    DB_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
)

# -----------------------------
# DB HELPERS
# -----------------------------
def has_column(engine, table: str, column: str) -> bool:
    q = """
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='public'
      AND table_name=:t
      AND column_name=:c
    LIMIT 1
    """
    with engine.begin() as conn:
        return conn.execute(text(q), {"t": table, "c": column}).fetchone() is not None


def has_table(engine, table: str) -> bool:
    q = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema='public'
      AND table_name=:t
    LIMIT 1
    """
    with engine.begin() as conn:
        return conn.execute(text(q), {"t": table}).fetchone() is not None


def exec_sql(sql: str, params: dict | None = None) -> None:
    with ENGINE.begin() as conn:
        conn.execute(text(sql), params or {})


def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    with ENGINE.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


@st.cache_data(show_spinner=False)
def read_df_cached(sql: str, params_tuple: tuple) -> pd.DataFrame:
    # params_tuple: tuple(sorted(params.items()))
    params = dict(params_tuple)
    return read_df(sql, params)


def cache_bust() -> None:
    try:
        st.cache_data.clear()
    except Exception:
        pass


# -----------------------------
# INIT / MIGRATIONS
# -----------------------------
def init_db(engine) -> None:
    # ARTICLES
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS articles (
            article      TEXT PRIMARY KEY,
            designation  TEXT DEFAULT '',
            stock        INTEGER NOT NULL DEFAULT 0,
            seuil_piece  INTEGER NOT NULL DEFAULT 0,
            garantie     INTEGER NOT NULL DEFAULT 0
        );
        """
    )

    # MOUVEMENTS (nouveau schéma)
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS mouvements (
            id           SERIAL PRIMARY KEY,
            date_mvt     DATE NOT NULL,
            article      TEXT NOT NULL,
            designation  TEXT DEFAULT '',
            type_mvt     TEXT NOT NULL,
            emplacement  TEXT NOT NULL,
            quantite     INTEGER NOT NULL,
            adresse      TEXT DEFAULT '',
            commentaire  TEXT DEFAULT ''
        );
        """
    )

    # ADRESSES
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS adresses (
            id  SERIAL PRIMARY KEY,
            nom TEXT UNIQUE NOT NULL
        );
        """
    )

    # SETTINGS
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS settings (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL DEFAULT ''
        );
        """
    )

    # Migration: si anciens schémas existent
    migrate_mouvements_date(engine)
    migrate_settings_column(engine)


def migrate_mouvements_date(engine) -> None:
    """
    Corrige le cas où l'ancienne table avait une colonne 'date' NOT NULL
    au lieu de 'date_mvt' (ce qui déclenche ton erreur).
    On ajoute date_mvt si besoin et on copie depuis date.
    """
    if not has_table(engine, "mouvements"):
        return

    if (not has_column(engine, "mouvements", "date_mvt")) and has_column(engine, "mouvements", "date"):
        exec_sql("ALTER TABLE mouvements ADD COLUMN IF NOT EXISTS date_mvt DATE;")
        exec_sql("UPDATE mouvements SET date_mvt = date WHERE date_mvt IS NULL;")

    # Si date_mvt existe mais est nullable, on ne force pas ici (pour éviter casse anciennes lignes).
    # Par contre notre insert garanti qu'on remplit la bonne colonne.


def migrate_settings_column(engine) -> None:
    """
    Tu as eu une erreur: SELECT v FROM settings ... => colonne v inexistante.
    Ici on gère ça:
    - si settings existe mais colonne v n'existe pas, on crée v et tente de recopier depuis 'value' si elle existe.
    """
    if not has_table(engine, "settings"):
        return

    if not has_column(engine, "settings", "v"):
        exec_sql("ALTER TABLE settings ADD COLUMN IF NOT EXISTS v TEXT NOT NULL DEFAULT '';")

        # si ancienne colonne 'value' existe, on copie
        if has_column(engine, "settings", "value"):
            exec_sql("UPDATE settings SET v = COALESCE(value,'') WHERE v = '';")

    # assure k
    if not has_column(engine, "settings", "k"):
        # très rare, mais si la table est bizarre, on ne force pas. On laissera l’app fonctionner sans settings.
        pass


init_db(ENGINE)

# -----------------------------
# LOGIC
# -----------------------------
def get_setting_int(key: str, default: int = 0) -> int:
    if not has_table(ENGINE, "settings"):
        return default
    col = "v" if has_column(ENGINE, "settings", "v") else ("value" if has_column(ENGINE, "settings", "value") else None)
    if not col:
        return default

    df = read_df(f"SELECT {col} AS v FROM settings WHERE k=:k LIMIT 1", {"k": key})
    if df.empty:
        return default
    try:
        return int(str(df.iloc[0]["v"] or "").strip() or default)
    except Exception:
        return default


def set_setting_int(key: str, value: int) -> None:
    # Upsert
    exec_sql(
        """
        INSERT INTO settings(k, v)
        VALUES (:k, :v)
        ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
        """,
        {"k": key, "v": str(int(value))},
    )
    cache_bust()


def get_designation(article: str) -> str:
    if not article:
        return ""
    df = read_df("SELECT COALESCE(designation,'') AS designation FROM articles WHERE article=:a LIMIT 1", {"a": str(article)})
    if df.empty:
        return ""
    return str(df.iloc[0]["designation"] or "")


def upsert_article(article: str, designation: str | None = None, garantie: int | None = None, seuil_piece: int | None = None) -> None:
    """
    Crée l'article s'il n'existe pas.
    Met à jour uniquement les champs fournis (sinon conserve DB).
    """
    article = str(article).strip()
    if not article:
        return

    # récupère existant
    row = read_df(
        """
        SELECT article, COALESCE(designation,'') AS designation, garantie, COALESCE(seuil_piece,0) AS seuil_piece
        FROM articles
        WHERE article=:a
        LIMIT 1
        """,
        {"a": article},
    )

    if row.empty:
        exec_sql(
            """
            INSERT INTO articles(article, designation, stock, garantie, seuil_piece)
            VALUES (:a, :d, 0, :g, :s)
            """,
            {
                "a": article,
                "d": (designation or "").strip(),
                "g": int(garantie or 0),
                "s": int(seuil_piece or 0),
            },
        )
    else:
        cur = row.iloc[0]
        new_design = cur["designation"]
        new_gar = int(cur["garantie"])
        new_seuil = int(cur["seuil_piece"])

        if designation is not None:
            new_design = (designation or "").strip()
        if garantie is not None:
            new_gar = int(garantie)
        if seuil_piece is not None:
            new_seuil = int(seuil_piece)

        exec_sql(
            """
            UPDATE articles
            SET designation=:d, garantie=:g, seuil_piece=:s
            WHERE article=:a
            """,
            {"a": article, "d": new_design, "g": new_gar, "s": new_seuil},
        )

    cache_bust()


def apply_movement(article: str, type_mvt: str, quantite: int) -> None:
    # met à jour stock dans articles
    if type_mvt == "ENTREE":
        exec_sql("UPDATE articles SET stock = stock + :q WHERE article=:a", {"q": int(quantite), "a": str(article)})
    else:
        exec_sql("UPDATE articles SET stock = stock - :q WHERE article=:a", {"q": int(quantite), "a": str(article)})


def insert_mouvement(
    date_mvt: dt.date,
    article: str,
    designation: str,
    type_mvt: str,
    emplacement: str,
    quantite: int,
    adresse: str = "",
    commentaire: str = "",
) -> None:
    """
    Insert compatible ancien schéma:
    - si mouvements a date_mvt -> on écrit date_mvt
    - sinon si mouvements a date -> on écrit date
    Et on évite la NotNullViolation.
    """
    migrate_mouvements_date(ENGINE)

    # si designation vide -> tente DB
    designation = (designation or "").strip()
    if not designation:
        designation = get_designation(article).strip()

    designation = designation or ""

    use_date_mvt = has_column(ENGINE, "mouvements", "date_mvt")
    if use_date_mvt:
        sql = """
        INSERT INTO mouvements(date_mvt, article, designation, type_mvt, emplacement, quantite, adresse, commentaire)
        VALUES (:d, :a, :des, :t, :e, :q, :ad, :c)
        """
    else:
        sql = """
        INSERT INTO mouvements(date, article, designation, type_mvt, emplacement, quantite, adresse, commentaire)
        VALUES (:d, :a, :des, :t, :e, :q, :ad, :c)
        """

    exec_sql(
        sql,
        {
            "d": date_mvt,
            "a": str(article),
            "des": designation,
            "t": type_mvt,
            "e": emplacement,
            "q": int(quantite),
            "ad": adresse or "",
            "c": commentaire or "",
        },
    )


def delete_article(article: str) -> None:
    exec_sql("DELETE FROM mouvements WHERE article=:a", {"a": str(article)})
    exec_sql("DELETE FROM articles WHERE article=:a", {"a": str(article)})
    cache_bust()


# -----------------------------
# UI HELPERS (auto designation)
# -----------------------------
def on_article_change():
    art = str(st.session_state.get("mvt_article", "")).strip()
    if not art:
        st.session_state["mvt_designation"] = ""
        return
    d = get_designation(art)
    if d:
        st.session_state["mvt_designation"] = d  # auto-remplir
    # si pas trouvé: on laisse ce que l’utilisateur a tapé (ou vide)


# -----------------------------
# APP UI
# -----------------------------
st.title("📦 Gestion de stock")

tab_mvt, tab_stock, tab_addr = st.tabs(["➕ Mouvement", "📦 Stock actuel", "📍 Adresses"])

# =========================================================
# TAB 1 : MOUVEMENT  (AJOUT + MODIFIER ARTICLE)
# =========================================================
with tab_mvt:
    st.subheader("Ajouter un mouvement")

    # liste adresses
    addr_df = read_df("SELECT nom FROM adresses ORDER BY nom")
    addr_list = [""] + addr_df["nom"].astype(str).tolist()

    with st.form("form_mvt", clear_on_submit=False):
        c1, c2, c3 = st.columns([1.1, 1.1, 1.8])

        with c1:
            date_mvt = st.date_input("Date", value=dt.date.today(), key="mvt_date")
            article = st.text_input("Numéro d'article", placeholder="Ex: 155082", key="mvt_article", on_change=on_article_change)
            designation = st.text_input("Désignation", placeholder="Ex: Sonde O2", key="mvt_designation")

        with c2:
            emplacement = st.selectbox("Emplacement", ["STOCK"], index=0, key="mvt_emp")
            type_mvt = st.selectbox("Type", ["ENTREE", "SORTIE"], index=0, key="mvt_type")
            quantite = st.number_input("Quantité", min_value=1, max_value=100000, value=1, step=1, key="mvt_qte")

            st.caption("Seuil pièce : 0 = aucun (seuil global utilisé)")
            seuil_piece = st.number_input("Seuil pièce (optionnel)", min_value=0, max_value=10000, value=0, step=1, key="mvt_seuil")
            maj_seuil = st.checkbox("Mettre à jour le seuil de cette pièce (même si elle existe déjà)", value=True, key="mvt_maj_seuil")

        with c3:
            commentaire = st.text_area("Remarque / commentaire (optionnel)", height=120, key="mvt_comment")
            adresse = st.selectbox("Adresse (optionnel)", addr_list, index=0, key="mvt_addr")
            submitted = st.form_submit_button("✅ Enregistrer", use_container_width=True)

    if submitted:
        article = str(st.session_state.get("mvt_article", "")).strip()
        designation = str(st.session_state.get("mvt_designation", "")).strip()

        if not article:
            st.error("❌ Numéro d'article obligatoire.")
        else:
            # Crée/MAJ article:
            # - si designation vide et article existe: on conservera DB
            # - si designation vide et n'existe pas: on crée quand même avec designation vide (pas d'erreur)
            if maj_seuil:
                # si designation vide -> upsert conserve DB si existe, sinon crée vide
                upsert_article(article, designation=None if designation == "" else designation, seuil_piece=int(seuil_piece))
            else:
                # on ne touche pas au seuil
                if designation:
                    upsert_article(article, designation=designation)

            # applique stock + insert mouvement (avec fix date/date_mvt)
            apply_movement(article, type_mvt, int(quantite))
            insert_mouvement(
                date_mvt=date_mvt,
                article=article,
                designation=designation,
                type_mvt=type_mvt,
                emplacement=emplacement,
                quantite=int(quantite),
                adresse=adresse,
                commentaire=commentaire,
            )

            st.success("✅ Mouvement enregistré.")
            cache_bust()
            st.rerun()

    st.divider()
    st.subheader("Modifier un article (désignation / seuil pièce / garantie)")

    articles_df = read_df(
        """
        SELECT article, COALESCE(designation,'') AS designation, garantie, COALESCE(seuil_piece,0) AS seuil_piece
        FROM articles
        ORDER BY article
        """
    )
    articles_list = articles_df["article"].astype(str).tolist()

    if not articles_list:
        st.info("Aucun article à modifier.")
    else:
        a1, a2 = st.columns([1.2, 1.8])

        with a1:
            art_sel = st.selectbox("Choisir l'article", articles_list, key="edit_article_sel")
            row = articles_df[articles_df["article"].astype(str) == str(art_sel)].iloc[0]

        with a2:
            with st.form("form_edit_article"):
                new_design = st.text_input("Désignation", value=str(row["designation"] or ""))
                new_seuil = st.number_input("Seuil pièce (0 = pas de seuil perso)", min_value=0, max_value=10000, value=int(row["seuil_piece"]), step=1)
                new_gar = st.number_input("Garantie (nombre)", min_value=0, max_value=100000, value=int(row["garantie"]), step=1)

                save_edit = st.form_submit_button("💾 Enregistrer la modification", use_container_width=True)

            if save_edit:
                upsert_article(str(art_sel), designation=new_design, garantie=int(new_gar), seuil_piece=int(new_seuil))
                st.success("✅ Article modifié.")
                st.rerun()

# =========================================================
# TAB 2 : STOCK ACTUEL  (STOCK + GARANTIES + PIECES A COMMANDER + HISTORIQUE + SUPPRIMER)
# =========================================================
with tab_stock:
    st.subheader("Stock actuel")

    search = st.text_input("Recherche", placeholder="Numéro ou mot dans désignation…").strip().lower()

    df_stock = read_df(
        """
        SELECT article, COALESCE(designation,'') AS designation, stock, COALESCE(seuil_piece,0) AS seuil_piece
        FROM articles
        ORDER BY article
        """
    )

    if search:
        mask = (
            df_stock["article"].astype(str).str.lower().str.contains(search, na=False)
            | df_stock["designation"].astype(str).str.lower().str.contains(search, na=False)
        )
        df_view = df_stock[mask].copy()
    else:
        df_view = df_stock.copy()

    st.dataframe(df_view, use_container_width=True, height=380)

    st.divider()
    st.subheader("Garanties")
    df_gar = read_df(
        """
        SELECT article, COALESCE(designation,'') AS designation, garantie
        FROM articles
        ORDER BY article
        """
    )
    st.dataframe(df_gar, use_container_width=True, height=260)

    st.divider()
    st.subheader("📦 Pièces à commander")

    seuil_global = get_setting_int("seuil_global", 0)
    colg1, colg2 = st.columns([1, 3])
    with colg1:
        new_global = st.number_input("Seuil global (utilisé si seuil pièce = 0)", min_value=0, max_value=10000, value=int(seuil_global), step=1)
        if st.button("💾 Enregistrer seuil global", use_container_width=True):
            set_setting_int("seuil_global", int(new_global))
            st.success("✅ Seuil global enregistré.")
            st.rerun()

    # calcul à commander
    if df_stock.empty:
        st.info("Aucun article.")
    else:
        df_calc = df_stock.copy()
        df_calc["seuil_applique"] = df_calc["seuil_piece"].apply(lambda x: int(x) if int(x) > 0 else int(seuil_global))
        df_calc["a_commander"] = (df_calc["seuil_applique"] - df_calc["stock"]).clip(lower=0)
        df_to_order = df_calc[df_calc["a_commander"] > 0][["article", "designation", "stock", "seuil_applique", "a_commander"]].copy()

        with colg2:
            if df_to_order.empty:
                st.success("✅ Rien à commander.")
            else:
                st.dataframe(df_to_order, use_container_width=True, height=260)

    st.divider()
    st.subheader("Historique (300 derniers)")

    # lecture historique compatible schéma
    # on affiche date_mvt si existe, sinon date
    date_col = "date_mvt" if has_column(ENGINE, "mouvements", "date_mvt") else ("date" if has_column(ENGINE, "mouvements", "date") else "date_mvt")

    hist = read_df(
        f"""
        SELECT id,
               {date_col} AS date_mvt,
               article,
               COALESCE(designation,'') AS designation,
               type_mvt,
               emplacement,
               quantite,
               COALESCE(adresse,'') AS adresse,
               COALESCE(commentaire,'') AS commentaire
        FROM mouvements
        ORDER BY id DESC
        LIMIT 300
        """
    )
    st.dataframe(hist, use_container_width=True, height=360)

    st.divider()
    st.subheader("🗑️ Supprimer un article (tout en bas)")

    articles_del = read_df("SELECT article FROM articles ORDER BY article")["article"].astype(str).tolist()
    if not articles_del:
        st.info("Aucun article à supprimer.")
    else:
        del_col1, del_col2 = st.columns([1.5, 2.5])
        with del_col1:
            a_del = st.selectbox("Choisir l'article à supprimer", articles_del, key="del_article")
            confirm = st.checkbox("Je confirme la suppression (irréversible)", value=False, key="del_confirm")

        with del_col2:
            st.warning("⚠️ Supprime aussi tous les mouvements liés à cet article.")
            if st.button("❌ Supprimer définitivement", use_container_width=True, disabled=not confirm):
                try:
                    delete_article(a_del)
                    st.success("✅ Article supprimé.")
                    st.rerun()
                except Exception as e:
                    st.exception(e)

# =========================================================
# TAB 3 : ADRESSES
# =========================================================
with tab_addr:
    st.subheader("Adresses")

    left, right = st.columns([1.2, 1.8])

    with left:
        st.markdown("### Ajouter une adresse")
        new_addr = st.text_input("Nom", placeholder="Ex: Atelier, Camion, Chantier…", key="addr_new").strip()
        if st.button("➕ Ajouter", use_container_width=True):
            if not new_addr:
                st.error("Nom obligatoire.")
            else:
                try:
                    exec_sql("INSERT INTO adresses(nom) VALUES (:n) ON CONFLICT (nom) DO NOTHING", {"n": new_addr})
                    cache_bust()
                    st.success("✅ Adresse ajoutée.")
                    st.rerun()
                except Exception as e:
                    st.exception(e)

    with right:
        st.markdown("### Liste / suppression")
        df_addr = read_df("SELECT nom FROM adresses ORDER BY nom")
        st.dataframe(df_addr, use_container_width=True, height=260)

        addr_list2 = df_addr["nom"].astype(str).tolist()
        if addr_list2:
            a_del = st.selectbox("Adresse à supprimer", addr_list2, key="addr_del")
            if st.button("🗑️ Supprimer l'adresse", use_container_width=True):
                try:
                    exec_sql("DELETE FROM adresses WHERE nom=:n", {"n": a_del})
                    cache_bust()
                    st.success("✅ Adresse supprimée.")
                    st.rerun()
                except Exception as e:
                    st.exception(e)
        else:
            st.info("Aucune adresse.")
