from __future__ import annotations

import datetime as dt
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# =========================
# CONFIG
# =========================
st.set_page_config(page_title="Gestion de stock", layout="wide")

APP_ICON = "📦"
ICON_COMMENT = "📝"
ICON_WARNING = "🛒"

DEFAULT_GLOBAL_SEUIL = 3


# =========================
# DB HELPERS
# =========================
@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    db_url = (st.secrets.get("DB_URL", "") or "").strip()
    if not db_url:
        st.error("DB_URL manquant. Ajoute DB_URL dans Streamlit → Settings → Secrets.")
        st.stop()

    # pool_pre_ping évite les connexions mortes (important sur Streamlit Cloud)
    return create_engine(db_url, pool_pre_ping=True, pool_recycle=1800)


ENGINE = get_engine()


def exec_sql(sql: str, params: Optional[Dict[str, Any]] = None) -> None:
    with ENGINE.begin() as conn:
        conn.execute(text(sql), params or {})


def read_df(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    with ENGINE.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def table_has_column(table: str, column: str) -> bool:
    q = """
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name=:t AND column_name=:c
    LIMIT 1
    """
    df = read_df(q, {"t": table, "c": column})
    return not df.empty


def init_db() -> None:
    # 1) tables
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )

    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS articles (
            article TEXT PRIMARY KEY,
            designation TEXT NOT NULL,
            stock INTEGER NOT NULL DEFAULT 0,
            garantie INTEGER NOT NULL DEFAULT 0
        );
        """
    )

    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS mouvements (
            id BIGSERIAL PRIMARY KEY,
            date DATE NOT NULL,
            article TEXT NOT NULL,
            designation TEXT NOT NULL,
            type_mvt TEXT NOT NULL,
            emplacement TEXT NOT NULL,
            quantite INTEGER NOT NULL,
            commentaire TEXT
        );
        """
    )

    # 2) migrations : colonnes manquantes (si ton ancien code a créé un schéma différent)
    # articles.seuil_piece (seuil par pièce)
    if not table_has_column("articles", "seuil_piece"):
        exec_sql("ALTER TABLE articles ADD COLUMN seuil_piece INTEGER;")

    # mouvements.commentaire (au cas où)
    if not table_has_column("mouvements", "commentaire"):
        exec_sql("ALTER TABLE mouvements ADD COLUMN commentaire TEXT;")

    # mouvements.emplacement (au cas où)
    if not table_has_column("mouvements", "emplacement"):
        exec_sql("ALTER TABLE mouvements ADD COLUMN emplacement TEXT NOT NULL DEFAULT 'STOCK';")

    # mouvements.type_mvt (au cas où)
    if not table_has_column("mouvements", "type_mvt"):
        exec_sql("ALTER TABLE mouvements ADD COLUMN type_mvt TEXT NOT NULL DEFAULT 'ENTREE';")

    # mouvements.quantite (au cas où)
    if not table_has_column("mouvements", "quantite"):
        exec_sql("ALTER TABLE mouvements ADD COLUMN quantite INTEGER NOT NULL DEFAULT 0;")

    # 3) index (sur une colonne qui existe vraiment)
    exec_sql("CREATE INDEX IF NOT EXISTS idx_mouvements_date ON mouvements(date);")
    exec_sql("CREATE INDEX IF NOT EXISTS idx_mouvements_article ON mouvements(article);")

    # 4) default setting
    if read_df("SELECT 1 FROM settings WHERE key='seuil_global' LIMIT 1").empty:
        exec_sql("INSERT INTO settings(key, value) VALUES('seuil_global', :v)", {"v": str(DEFAULT_GLOBAL_SEUIL)})


init_db()


# =========================
# SETTINGS
# =========================
def get_setting(key: str, default: str) -> str:
    df = read_df("SELECT value FROM settings WHERE key=:k", {"k": key})
    if df.empty:
        return default
    return str(df.iloc[0]["value"])


def set_setting(key: str, value: str) -> None:
    exec_sql(
        """
        INSERT INTO settings(key, value)
        VALUES(:k, :v)
        ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
        """,
        {"k": key, "v": value},
    )


# =========================
# CACHE (lecture)
# =========================
@st.cache_data(show_spinner=False)
def load_articles() -> pd.DataFrame:
    return read_df(
        """
        SELECT article, designation, stock, garantie, seuil_piece
        FROM articles
        ORDER BY article
        """
    )


@st.cache_data(show_spinner=False)
def load_mouvements(limit: int = 300) -> pd.DataFrame:
    return read_df(
        """
        SELECT id, date, article, designation, type_mvt, emplacement, quantite, commentaire
        FROM mouvements
        ORDER BY id DESC
        LIMIT :lim
        """,
        {"lim": int(limit)},
    )


def clear_cache() -> None:
    load_articles.clear()
    load_mouvements.clear()


# =========================
# BUSINESS LOGIC
# =========================
def upsert_article(article: str, designation: str, seuil_piece: Optional[int]) -> None:
    exec_sql(
        """
        INSERT INTO articles(article, designation, seuil_piece)
        VALUES(:a, :d, :s)
        ON CONFLICT(article) DO UPDATE
        SET designation = EXCLUDED.designation,
            seuil_piece = EXCLUDED.seuil_piece
        """,
        {"a": article, "d": designation, "s": seuil_piece},
    )


def adjust_stock(article: str, emplacement: str, delta: int) -> None:
    col = "stock" if emplacement == "STOCK" else "garantie"
    exec_sql(f"UPDATE articles SET {col} = {col} + :x WHERE article=:a", {"x": int(delta), "a": article})


def get_designation(article: str) -> str:
    df = read_df("SELECT designation FROM articles WHERE article=:a", {"a": article})
    return "" if df.empty else str(df.iloc[0]["designation"])


def add_mouvement(date_: dt.date, article: str, designation: str, type_mvt: str, emplacement: str, qty: int, commentaire: str) -> None:
    exec_sql(
        """
        INSERT INTO mouvements(date, article, designation, type_mvt, emplacement, quantite, commentaire)
        VALUES(:dt, :a, :d, :t, :e, :q, :c)
        """,
        {"dt": date_, "a": article, "d": designation, "t": type_mvt, "e": emplacement, "q": int(qty), "c": (commentaire or "").strip() or None},
    )


def delete_article(article: str) -> None:
    # supprime mouvements puis article
    exec_sql("DELETE FROM mouvements WHERE article=:a", {"a": article})
    exec_sql("DELETE FROM articles WHERE article=:a", {"a": article})


# =========================
# UI HELPERS (style)
# =========================
def build_stock_view(df: pd.DataFrame, global_seuil: int) -> pd.DataFrame:
    out = df.copy()

    # seuil effectif = seuil_piece si présent sinon seuil_global
    out["seuil_effectif"] = out["seuil_piece"].fillna(global_seuil).astype(int)

    # “à commander” si stock <= seuil_effectif
    out["a_commander"] = out["stock"] <= out["seuil_effectif"]

    # icône
    out[""] = out.apply(
        lambda r: (ICON_WARNING if bool(r["a_commander"]) else ""),
        axis=1,
    )

    # colonnes visibles
    out = out[["", "article", "designation", "stock", "seuil_piece", "seuil_effectif"]]
    out = out.rename(
        columns={
            "seuil_piece": "Seuil pièce",
            "seuil_effectif": "Seuil utilisé",
        }
    )
    return out


def style_stock(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    def color_warn(row):
        # colonne "" contient l’icône 🛒 quand à commander
        if str(row.get("", "")) == ICON_WARNING:
            return ["font-weight:700;"] * len(row)
        return [""] * len(row)

    return df.style.apply(color_warn, axis=1)


def build_hist_view(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out[""] = out["commentaire"].apply(lambda c: ICON_COMMENT if (isinstance(c, str) and c.strip()) else "")
    out = out[["", "date", "article", "designation", "type_mvt", "emplacement", "quantite", "commentaire"]]
    return out


def style_hist(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    def red_comment(val):
        if isinstance(val, str) and val.strip():
            return "color: red; font-weight: 700;"
        return ""

    return df.style.applymap(red_comment, subset=["commentaire"])


# =========================
# APP
# =========================
st.title(f"{APP_ICON} Gestion de stock")

tabs = st.tabs(["➕ Mouvement", "📦 Stock actuel", "🗑️ Supprimer un article (tout en bas)"])

# -------------------------
# TAB 1: Mouvement
# -------------------------
with tabs[0]:
    st.subheader("Ajouter un mouvement")

    col1, col2, col3 = st.columns([1.2, 1.0, 1.3])

    with col1:
        date_mvt = st.date_input("Date", value=dt.date.today())
        article = st.text_input("Numéro d'article", placeholder="Ex: 155082").strip()

        # ⚠️ Ici tu choisis le seuil de CETTE pièce (au moment où tu l’ajoutes / modifies)
        seuil_piece = st.number_input(
            "Seuil pièce (optionnel) — limite pour 'Pièces à commander'",
            min_value=0,
            max_value=10_000,
            value=0,
            step=1,
            help="Si tu mets 0, on considère que tu ne veux pas de seuil pièce (on utilisera le seuil global).",
        )
        seuil_piece_db: Optional[int] = None if int(seuil_piece) == 0 else int(seuil_piece)

    with col2:
        emplacement = st.selectbox("Emplacement", ["STOCK", "GARANTIE"])
        type_mvt = st.selectbox("Type", ["ENTREE", "SORTIE"])
        qty = st.number_input("Quantité", min_value=1, max_value=10_000, value=1, step=1)

        # designation auto si article existe
        auto_des = get_designation(article) if article else ""
        designation = st.text_input("Désignation", value=auto_des, placeholder="Ex: Sonde O2")

    with col3:
        commentaire = st.text_area("Remarque / commentaire (optionnel)")
        save = st.button("✅ Enregistrer", use_container_width=True)

    if save:
        if not article:
            st.error("Tu dois remplir le numéro d’article.")
        elif not designation.strip():
            st.error("Tu dois remplir la désignation (au moins la 1ère fois).")
        else:
            # 1) upsert article + seuil_piece
            upsert_article(article=article, designation=designation.strip(), seuil_piece=seuil_piece_db)

            # 2) stock delta
            delta = int(qty) if type_mvt == "ENTREE" else -int(qty)
            adjust_stock(article=article, emplacement=emplacement, delta=delta)

            # 3) mouvement
            add_mouvement(
                date_=date_mvt,
                article=article,
                designation=designation.strip(),
                type_mvt=type_mvt,
                emplacement=emplacement,
                qty=int(qty),
                commentaire=commentaire,
            )

            clear_cache()
            st.success("Mouvement enregistré ✅")

# -------------------------
# TAB 2: Stock actuel
# -------------------------
with tabs[1]:
    st.subheader("Stock actuel")

    df_articles = load_articles()

    # barre de recherche au-dessus
    search = st.text_input("🔎 Rechercher par numéro d’article", placeholder="Tape un numéro (ex: 155082)")
    if search.strip():
        df_view = df_articles[df_articles["article"].astype(str).str.contains(search.strip(), case=False, na=False)].copy()
        if df_view.empty:
            st.warning("Aucun article trouvé.")
    else:
        df_view = df_articles.copy()

    # seuil global (si la pièce n’a pas de seuil_piece)
    colA, colB = st.columns([1, 2])
    with colA:
        global_seuil = int(get_setting("seuil_global", str(DEFAULT_GLOBAL_SEUIL)))
        new_global = st.number_input(
            "Seuil global (utilisé si 'Seuil pièce' est vide)",
            min_value=0,
            max_value=10_000,
            value=int(global_seuil),
            step=1,
        )
        if int(new_global) != int(global_seuil):
            set_setting("seuil_global", str(int(new_global)))
            global_seuil = int(new_global)
            st.info("Seuil global mis à jour ✅")

    # table stock
    stock_table = build_stock_view(df_view, global_seuil=global_seuil)

    st.dataframe(
        style_stock(stock_table),
        use_container_width=True,
        height=520,
    )

    st.divider()

    # pièces à commander
    st.subheader(f"{ICON_WARNING} Pièces à commander")
    df_full = load_articles()
    view_full = build_stock_view(df_full, global_seuil=global_seuil)
    a_commander = view_full[view_full[""] == ICON_WARNING].copy()

    if a_commander.empty:
        st.success("Rien à commander ✅")
    else:
        st.dataframe(style_stock(a_commander), use_container_width=True, height=360)

    st.divider()

    # historique
    st.subheader("Historique")
    hist = load_mouvements(300)
    if hist.empty:
        st.info("Aucun mouvement pour l’instant.")
    else:
        hist_view = build_hist_view(hist)
        st.dataframe(style_hist(hist_view), use_container_width=True, height=420)

# -------------------------
# TAB 3: Supprimer (tout en bas)
# -------------------------
with tabs[2]:
    st.subheader("Supprimer un article (irréversible)")
    df_articles = load_articles()
    if df_articles.empty:
        st.info("Aucun article.")
    else:
        choices = df_articles["article"].tolist()
        art_del = st.selectbox("Article à supprimer", choices)
        st.warning("Supprime l’article + son historique. Action irréversible.")
        if st.button("🗑️ Supprimer définitivement", type="primary"):
            delete_article(art_del)
            clear_cache()
            st.success(f"Article {art_del} supprimé ✅")
