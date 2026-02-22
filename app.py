# app.py
import os
import datetime as dt

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Gestion de stock", page_icon="📦", layout="wide")


# -------------------------
# CONFIG / DB
# -------------------------
def get_db_url() -> str:
    # Priorité: Streamlit secrets, sinon variable d'env
    if "DB_URL" in st.secrets:
        return st.secrets["DB_URL"]
    return os.environ.get("DB_URL", "")


@st.cache_resource
def get_engine():
    db_url = get_db_url().strip()
    if not db_url:
        st.error("❌ DB_URL manquant. Ajoute DB_URL dans Streamlit Secrets.")
        st.stop()

    # Sécurise le format (certains collent postgres://postgresql://)
    db_url = db_url.replace("postgresql://postgresql://", "postgresql://")

    return create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        future=True,
    )


ENGINE = get_engine()


def exec_sql(sql: str, params: dict | None = None) -> None:
    params = params or {}
    with ENGINE.begin() as conn:
        conn.execute(text(sql), params)


def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    params = params or {}
    with ENGINE.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def cache_bust():
    st.session_state["cache_token"] = st.session_state.get("cache_token", 0) + 1


# -------------------------
# INIT DB (tables + colonnes)
# -------------------------
def init_db():
    # Tables
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS articles (
            article      TEXT PRIMARY KEY,
            designation  TEXT NOT NULL DEFAULT '',
            stock        INTEGER NOT NULL DEFAULT 0,
            seuil_piece  INTEGER NOT NULL DEFAULT 0,
            garantie     INTEGER NOT NULL DEFAULT 0
        );
        """
    )

    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS mouvements (
            id          BIGSERIAL PRIMARY KEY,
            date_mvt    DATE NOT NULL,
            article     TEXT NOT NULL,
            designation TEXT NOT NULL DEFAULT '',
            type_mvt    TEXT NOT NULL,
            emplacement TEXT NOT NULL DEFAULT 'STOCK',
            quantite    INTEGER NOT NULL,
            adresse     TEXT NOT NULL DEFAULT '',
            commentaire TEXT NOT NULL DEFAULT ''
        );
        """
    )

    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS adresses (
            id   BIGSERIAL PRIMARY KEY,
            nom  TEXT UNIQUE NOT NULL
        );
        """
    )

    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS settings (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        );
        """
    )

    # Colonnes manquantes (au cas où ta DB est ancienne)
    # Postgres: ADD COLUMN IF NOT EXISTS OK
    exec_sql("ALTER TABLE articles   ADD COLUMN IF NOT EXISTS seuil_piece INTEGER NOT NULL DEFAULT 0;")
    exec_sql("ALTER TABLE articles   ADD COLUMN IF NOT EXISTS garantie    INTEGER NOT NULL DEFAULT 0;")
    exec_sql("ALTER TABLE articles   ADD COLUMN IF NOT EXISTS stock       INTEGER NOT NULL DEFAULT 0;")
    exec_sql("ALTER TABLE articles   ADD COLUMN IF NOT EXISTS designation TEXT NOT NULL DEFAULT '';")

    exec_sql("ALTER TABLE mouvements ADD COLUMN IF NOT EXISTS adresse     TEXT NOT NULL DEFAULT '';")
    exec_sql("ALTER TABLE mouvements ADD COLUMN IF NOT EXISTS commentaire TEXT NOT NULL DEFAULT '';")
    exec_sql("ALTER TABLE mouvements ADD COLUMN IF NOT EXISTS emplacement TEXT NOT NULL DEFAULT 'STOCK';")

    # Index utiles
    exec_sql("CREATE INDEX IF NOT EXISTS idx_mouvements_date ON mouvements(date_mvt);")
    exec_sql("CREATE INDEX IF NOT EXISTS idx_mouvements_article ON mouvements(article);")


init_db()


# -------------------------
# SETTINGS helpers
# -------------------------
def get_setting_int(key: str, default: int) -> int:
    df = read_df("SELECT v FROM settings WHERE k=:k", {"k": key})
    if df.empty:
        return default
    try:
        return int(df.iloc[0]["v"])
    except Exception:
        return default


def set_setting_int(key: str, value: int):
    exec_sql(
        """
        INSERT INTO settings(k, v)
        VALUES(:k, :v)
        ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
        """,
        {"k": key, "v": str(int(value))},
    )


# -------------------------
# STOCK / ARTICLES helpers
# -------------------------
def upsert_article_min(article: str, designation: str = ""):
    exec_sql(
        """
        INSERT INTO articles(article, designation)
        VALUES(:a, :d)
        ON CONFLICT (article) DO UPDATE SET
            designation = CASE
                WHEN EXCLUDED.designation <> '' THEN EXCLUDED.designation
                ELSE articles.designation
            END
        """,
        {"a": article, "d": designation or ""},
    )


def update_article_fields(article: str, designation: str, seuil_piece: int, garantie: int):
    exec_sql(
        """
        UPDATE articles
        SET designation=:d,
            seuil_piece=:s,
            garantie=:g
        WHERE article=:a
        """,
        {"a": article, "d": designation or "", "s": int(seuil_piece), "g": int(garantie)},
    )


def apply_movement(article: str, type_mvt: str, quantite: int):
    q = int(quantite)
    if q <= 0:
        raise ValueError("Quantité invalide")

    if type_mvt == "ENTREE":
        exec_sql("UPDATE articles SET stock = stock + :q WHERE article=:a", {"q": q, "a": article})
    else:
        # sortie: empêcher stock négatif
        cur = read_df("SELECT stock FROM articles WHERE article=:a", {"a": article})
        stock_now = int(cur.iloc[0]["stock"]) if not cur.empty else 0
        if stock_now - q < 0:
            raise ValueError(f"Stock insuffisant (stock={stock_now}, sortie={q})")
        exec_sql("UPDATE articles SET stock = stock - :q WHERE article=:a", {"q": q, "a": article})


def insert_movement(
    date_mvt: dt.date,
    article: str,
    designation: str,
    type_mvt: str,
    emplacement: str,
    quantite: int,
    adresse: str,
    commentaire: str,
):
    exec_sql(
        """
        INSERT INTO mouvements(date_mvt, article, designation, type_mvt, emplacement, quantite, adresse, commentaire)
        VALUES(:date_mvt, :article, :designation, :type_mvt, :emplacement, :quantite, :adresse, :commentaire)
        """,
        {
            "date_mvt": date_mvt,
            "article": article,
            "designation": designation or "",
            "type_mvt": type_mvt,
            "emplacement": emplacement or "STOCK",
            "quantite": int(quantite),
            "adresse": adresse or "",
            "commentaire": commentaire or "",
        },
    )


def delete_article(article: str):
    exec_sql("DELETE FROM mouvements WHERE article=:a", {"a": article})
    exec_sql("DELETE FROM articles   WHERE article=:a", {"a": article})


# -------------------------
# UI
# -------------------------
st.title("📦 Gestion de stock")

tab_mvt, tab_stock, tab_addr = st.tabs(["➕ Mouvement", "📦 Stock actuel", "📍 Adresses"])


# =========================
# TAB 1 : MOUVEMENT
# =========================
with tab_mvt:
    st.subheader("Ajouter un mouvement")

    # Liste adresses
    addr_df = read_df("SELECT nom FROM adresses ORDER BY nom")
    addr_list = [""] + addr_df["nom"].tolist()

    def get_designation_for_article(article: str) -> str:
    if not article:
        return ""
    df = read_df(
        "SELECT designation FROM articles WHERE article=:a",
        {"a": article},
        bust=st.session_state.get("_cache_bust", 0),
    )
    if df.empty:
        return ""
    return str(df.iloc[0]["designation"] or "")

def on_article_change():
    a = (st.session_state.get("mvt_article") or "").strip()
    # si l'utilisateur n'a rien tapé en désignation, on auto-remplit depuis la DB
    if a and not (st.session_state.get("mvt_designation") or "").strip():
        st.session_state["mvt_designation"] = get_designation_for_article(a)

    with st.form("form_mvt", clear_on_submit=False):
        c1, c2, c3 = st.columns([1.1, 1.1, 1.8])

        with c1:
            date_mvt = st.date_input("Date", value=dt.date.today())
            article = st.text_input("Numéro d'article", placeholder="Ex: 155082").strip()
            designation = st.text_input("Désignation", placeholder="Ex: Sonde O2").strip()

        with c2:
            emplacement = st.selectbox("Emplacement", ["STOCK"], index=0)
            type_mvt = st.selectbox("Type", ["ENTREE", "SORTIE"], index=0)
            quantite = st.number_input("Quantité", min_value=1, max_value=10_000, value=1, step=1)

            st.caption("Seuil pièce : 0 = aucun (seuil global utilisé)")
            seuil_piece = st.number_input(
                "Seuil pièce (optionnel)",
                min_value=0,
                max_value=10_000,
                value=0,
                step=1,
            )
            maj_seuil = st.checkbox("Mettre à jour le seuil de cette pièce (même si elle existe déjà)", value=True)

        with c3:
            commentaire = st.text_area("Remarque / commentaire (optionnel)", height=120)
            adresse = st.selectbox("Adresse (optionnel)", addr_list, index=0)

        submitted = st.form_submit_button("✅ Enregistrer", use_container_width=True)

    if submitted:
        if not article:
            st.error("❌ Numéro d'article obligatoire.")
        else:
            try:
                # crée ou maj article (désignation si donnée)
                upsert_article_min(article, designation)

                # maj seuil si demandé
                if maj_seuil:
                    exec_sql(
                        """
                        INSERT INTO articles(article, designation, stock, seuil_piece, garantie)
                        VALUES(:a, '', 0, :s, 0)
                        ON CONFLICT (article) DO UPDATE SET seuil_piece = EXCLUDED.seuil_piece
                        """,
                        {"a": article, "s": int(seuil_piece)},
                    )

                # applique mouvement + log
                apply_movement(article, type_mvt, int(quantite))

                # Pour le log, si désignation vide -> récupère en DB
                if not designation:
                    ddf = read_df("SELECT designation FROM articles WHERE article=:a", {"a": article})
                    designation_log = ddf.iloc[0]["designation"] if not ddf.empty else ""
                else:
                    designation_log = designation

                insert_movement(
                    date_mvt=date_mvt,
                    article=article,
                    designation=designation_log,
                    type_mvt=type_mvt,
                    emplacement=emplacement,
                    quantite=int(quantite),
                    adresse=adresse,
                    commentaire=commentaire,
                )

                cache_bust()
                st.success("✅ Mouvement enregistré.")
            except Exception as e:
                st.exception(e)

    st.divider()

    st.subheader("Modifier un article (désignation / seuil pièce / garantie)")
    articles_df = read_df(
        """
        SELECT article, designation, COALESCE(seuil_piece,0) AS seuil_piece, COALESCE(garantie,0) AS garantie
        FROM articles
        ORDER BY article
        """
    )
    articles_list = articles_df["article"].astype(str).tolist()

    if not articles_list:
        st.info("Aucun article à modifier.")
    else:
        colA, colB = st.columns([1.2, 1.8])

        with colA:
            art_sel = st.selectbox("Choisir l'article", articles_list, key="edit_article_sel")
            row = articles_df[articles_df["article"].astype(str) == str(art_sel)].iloc[0]

        with colB:
            with st.form("form_edit_article"):
                new_design = st.text_input("Désignation", value=str(row["designation"] or ""))
                new_seuil = st.number_input(
                    "Seuil pièce (0 = pas de seuil perso)",
                    min_value=0,
                    max_value=10_000,
                    value=int(row["seuil_piece"]),
                    step=1,
                )
                new_gar = st.number_input(
                    "Garantie (nombre)",
                    min_value=0,
                    max_value=10_000,
                    value=int(row["garantie"]),
                    step=1,
                )
                save_edit = st.form_submit_button("💾 Enregistrer la modification", use_container_width=True)

            if save_edit:
                try:
                    update_article_fields(str(art_sel), new_design.strip(), int(new_seuil), int(new_gar))
                    cache_bust()
                    st.success("✅ Article modifié.")
                except Exception as e:
                    st.exception(e)


# =========================
# TAB 2 : STOCK ACTUEL
# =========================
with tab_stock:
    st.subheader("Stock actuel")

    search = st.text_input("Recherche", placeholder="Numéro ou mot dans désignation...").strip().lower()

    df_stock = read_df(
        """
        SELECT article, designation, stock, COALESCE(seuil_piece,0) AS seuil_piece
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

    # ✅ PAS de colonne garantie ici
    st.dataframe(df_view, use_container_width=True, height=360)

    st.divider()

    # -------- Tableau garanties (à la place de la colonne) --------
    st.subheader("🛡️ Tableau garanties")
    df_gar = read_df(
        """
        SELECT article, designation, COALESCE(garantie,0) AS garantie
        FROM articles
        WHERE COALESCE(garantie,0) > 0
        ORDER BY garantie DESC, article
        """
    )
    if df_gar.empty:
        st.info("Aucune garantie renseignée.")
    else:
        st.dataframe(df_gar, use_container_width=True, height=240)

    st.divider()

    # -------- Historique ici (PAS dans Mouvement) --------
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
    st.dataframe(hist, use_container_width=True, height=320)

    st.divider()

    # -------- Pièces à commander --------
    st.subheader("📦 Pièces à commander")

    seuil_global = get_setting_int("seuil_global", 0)
    new_global = st.number_input(
        "Seuil global (utilisé si seuil pièce = 0)",
        min_value=0,
        max_value=10_000,
        value=int(seuil_global),
        step=1,
    )
    if new_global != seuil_global:
        set_setting_int("seuil_global", int(new_global))
        seuil_global = int(new_global)

    # seuil utilisé = seuil_piece si >0 sinon seuil_global
    df_cmd = df_stock.copy()
    df_cmd["seuil_utilise"] = df_cmd["seuil_piece"].apply(lambda x: int(x) if int(x) > 0 else int(seuil_global))
    df_cmd = df_cmd[df_cmd["stock"] <= df_cmd["seuil_utilise"]].copy()
    df_cmd = df_cmd.sort_values(["stock", "seuil_utilise", "article"], ascending=[True, False, True])

    if df_cmd.empty:
        st.success("✅ Rien à commander.")
    else:
        st.dataframe(
            df_cmd[["article", "designation", "stock", "seuil_utilise"]],
            use_container_width=True,
            height=260,
        )

    st.divider()

    # -------- Supprimer un article (tout en bas) --------
    st.subheader("🗑️ Supprimer un article (tout en bas)")
    st.warning("⚠️ Supprime aussi tous les mouvements liés à cet article.")

    articles_only = read_df("SELECT article FROM articles ORDER BY article")
    articles_list = articles_only["article"].astype(str).tolist()

    if not articles_list:
        st.info("Aucun article à supprimer.")
    else:
        a_del = st.selectbox("Choisir l'article à supprimer", articles_list, key="del_article")
        confirm = st.checkbox("Je confirme la suppression définitive", value=False)
        if st.button("❌ Supprimer définitivement", use_container_width=True, disabled=not confirm):
            try:
                delete_article(a_del)
                cache_bust()
                st.success("✅ Article supprimé.")
                st.rerun()
            except Exception as e:
                st.exception(e)


# =========================
# TAB 3 : ADRESSES
# =========================
with tab_addr:
    st.subheader("Adresses")

    c1, c2 = st.columns([1.2, 1.0])
    with c1:
        new_addr = st.text_input("Ajouter une adresse", placeholder="Ex: Chantier A / Atelier / Client X").strip()
        if st.button("➕ Ajouter", use_container_width=True):
            if not new_addr:
                st.error("Adresse vide.")
            else:
                try:
                    exec_sql("INSERT INTO adresses(nom) VALUES(:n) ON CONFLICT (nom) DO NOTHING", {"n": new_addr})
                    cache_bust()
                    st.success("✅ Adresse ajoutée.")
                    st.rerun()
                except Exception as e:
                    st.exception(e)

    with c2:
        df_addr = read_df("SELECT nom FROM adresses ORDER BY nom")
        st.caption("Liste des adresses")
        st.dataframe(df_addr, use_container_width=True, height=220)

    st.divider()

    st.subheader("Supprimer une adresse")
    df_addr2 = read_df("SELECT nom FROM adresses ORDER BY nom")
    addr_list2 = df_addr2["nom"].tolist()

    if not addr_list2:
        st.info("Aucune adresse à supprimer.")
    else:
        a = st.selectbox("Adresse à supprimer", addr_list2, key="addr_del")
        conf = st.checkbox("Je confirme la suppression", value=False, key="addr_conf")
        if st.button("🗑️ Supprimer l'adresse", use_container_width=True, disabled=not conf):
            try:
                exec_sql("DELETE FROM adresses WHERE nom=:n", {"n": a})
                cache_bust()
                st.success("✅ Adresse supprimée.")
                st.rerun()
            except Exception as e:
                st.exception(e)

