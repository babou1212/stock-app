# app.py
# ============================================================
# ✅ APP COMPLETE "Gestion de stock" (Streamlit + PostgreSQL)
#
# Objectifs / Correctifs inclus :
# - Onglet "Mouvement" : Ajouter un mouvement + Modifier un article
# - Onglet "Stock actuel" : Stock (SANS colonne garantie) + Pièces à commander
#                           + Historique (300) + Tableau Garantie + Supprimer un article (en bas)
# - Onglet "Adresses" : Ajouter / Supprimer
#
# ✅ Fix erreurs rencontrées :
# 1) settings.v manquant -> migration auto (ajoute colonne v)
# 2) ON CONFLICT (nom) sur adresses -> index UNIQUE auto créé
# 3) mouvements : DB parfois a colonne "date" au lieu de "date_mvt"
#    -> détection auto + INSERT sur la bonne colonne
# 4) si article existant -> désignation auto-remplie (callback Streamlit)
# 5) si article inconnu et désignation vide -> message d’erreur propre (pas de crash)
# ============================================================

from __future__ import annotations

import os
import datetime as dt
from typing import Optional, Dict, Any, List

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ----------------------------
# Streamlit config
# ----------------------------
st.set_page_config(page_title="Gestion de stock", page_icon="📦", layout="wide")


# ----------------------------
# DB Helpers
# ----------------------------
def get_db_url() -> str:
    url = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL")
    if not url:
        st.error("❌ Variable d'environnement DATABASE_URL manquante.")
        st.stop()

    # Streamlit Cloud fournit parfois postgres://
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]
    return url


@st.cache_resource
def get_engine() -> Engine:
    return create_engine(get_db_url(), pool_pre_ping=True)


ENGINE = get_engine()


def exec_sql(sql: str, params: Optional[Dict[str, Any]] = None) -> None:
    params = params or {}
    with ENGINE.begin() as conn:
        conn.execute(text(sql), params)


def read_df(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    params = params or {}
    with ENGINE.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def table_exists(table: str) -> bool:
    df = read_df(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='public' AND table_name=:t
        ) AS ok
        """,
        {"t": table},
    )
    return bool(df.iloc[0]["ok"])


def get_table_columns(table: str) -> List[str]:
    if not table_exists(table):
        return []
    df = read_df(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=:t
        ORDER BY ordinal_position
        """,
        {"t": table},
    )
    return df["column_name"].astype(str).tolist()


def cache_bust() -> None:
    st.cache_data.clear()


# ----------------------------
# DB init + migrations
# ----------------------------
def init_db() -> None:
    # ARTICLES
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS articles (
            article TEXT PRIMARY KEY,
            designation TEXT,
            stock INTEGER NOT NULL DEFAULT 0,
            seuil_piece INTEGER NOT NULL DEFAULT 0,
            garantie INTEGER NOT NULL DEFAULT 0
        );
        """
    )

    # MOUVEMENTS (nouvelle version = date_mvt)
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS mouvements (
            id SERIAL PRIMARY KEY,
            date_mvt DATE NOT NULL,
            article TEXT NOT NULL REFERENCES articles(article) ON DELETE CASCADE,
            designation TEXT,
            type_mvt TEXT NOT NULL,
            emplacement TEXT NOT NULL,
            quantite INTEGER NOT NULL,
            adresse TEXT NOT NULL DEFAULT '',
            commentaire TEXT NOT NULL DEFAULT ''
        );
        """
    )

    # ADRESSES
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS adresses (
            id SERIAL PRIMARY KEY,
            nom TEXT NOT NULL
        );
        """
    )
    # ON CONFLICT(nom) => doit être UNIQUE
    exec_sql("CREATE UNIQUE INDEX IF NOT EXISTS idx_adresses_nom_unique ON adresses(nom);")

    # SETTINGS (k/v)
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS settings (
            k TEXT PRIMARY KEY
        );
        """
    )
    cols = get_table_columns("settings")
    if "v" not in cols:
        exec_sql("ALTER TABLE settings ADD COLUMN IF NOT EXISTS v TEXT;")

    # valeur par défaut
    exec_sql(
        """
        INSERT INTO settings(k, v)
        VALUES ('seuil_global', '0')
        ON CONFLICT (k) DO NOTHING;
        """
    )


init_db()


# ----------------------------
# Compatibility: mouvements date column (date_mvt or date)
# ----------------------------
@st.cache_data
def get_mvt_date_column_name() -> str:
    cols = get_table_columns("mouvements")
    # vieille table : colonne "date" (NOT NULL) au lieu de "date_mvt"
    if "date" in cols and "date_mvt" not in cols:
        return "date"
    return "date_mvt"


# ----------------------------
# SETTINGS Helpers
# ----------------------------
def get_setting_int(key: str, default: int = 0) -> int:
    cols = get_table_columns("settings")
    if "v" not in cols:
        return default
    df = read_df("SELECT v FROM settings WHERE k=:k", {"k": key})
    if df.empty or df.iloc[0]["v"] is None:
        return default
    try:
        return int(str(df.iloc[0]["v"]).strip())
    except Exception:
        return default


def set_setting_int(key: str, value: int) -> None:
    exec_sql(
        """
        INSERT INTO settings(k, v)
        VALUES (:k, :v)
        ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
        """,
        {"k": key, "v": str(int(value))},
    )


# ----------------------------
# ARTICLES Helpers
# ----------------------------
def normalize_article(a: str) -> str:
    return (a or "").strip()


def get_article_row(article: str) -> Optional[Dict[str, Any]]:
    a = normalize_article(article)
    if not a:
        return None
    df = read_df(
        """
        SELECT article, designation, stock, seuil_piece, garantie
        FROM articles
        WHERE article=:a
        """,
        {"a": a},
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def get_article_designation(article: str) -> Optional[str]:
    row = get_article_row(article)
    if not row:
        return None
    d = row.get("designation")
    if d is None:
        return None
    d = str(d).strip()
    return d if d else None


def upsert_article(article: str, designation: Optional[str], seuil_piece: Optional[int]) -> None:
    a = normalize_article(article)
    d = (designation or "").strip()

    # créer si absent
    exec_sql(
        """
        INSERT INTO articles(article, designation, stock, seuil_piece, garantie)
        VALUES (:a, NULLIF(:d,''), 0, 0, 0)
        ON CONFLICT (article) DO NOTHING
        """,
        {"a": a, "d": d},
    )

    # update designation si donnée
    if d:
        exec_sql("UPDATE articles SET designation=:d WHERE article=:a", {"a": a, "d": d})

    # update seuil_piece si demandé
    if seuil_piece is not None:
        exec_sql("UPDATE articles SET seuil_piece=:s WHERE article=:a", {"a": a, "s": int(seuil_piece)})


def update_article_fields(article: str, designation: str, seuil_piece: int, garantie: int) -> None:
    a = normalize_article(article)
    d = (designation or "").strip()
    if not a:
        raise ValueError("Article vide")
    if not d:
        raise ValueError("Désignation vide")

    exec_sql(
        """
        UPDATE articles
        SET designation=:d,
            seuil_piece=:s,
            garantie=:g
        WHERE article=:a
        """,
        {"a": a, "d": d, "s": int(seuil_piece), "g": int(garantie)},
    )


def delete_article(article: str) -> None:
    a = normalize_article(article)
    if not a:
        return
    exec_sql("DELETE FROM articles WHERE article=:a", {"a": a})


# ----------------------------
# STOCK / MOUVEMENTS Helpers
# ----------------------------
def apply_movement(article: str, type_mvt: str, quantite: int) -> None:
    a = normalize_article(article)
    q = int(quantite)
    if q <= 0:
        raise ValueError("Quantité invalide")

    if type_mvt == "ENTREE":
        exec_sql("UPDATE articles SET stock = stock + :q WHERE article=:a", {"a": a, "q": q})
    elif type_mvt == "SORTIE":
        exec_sql("UPDATE articles SET stock = stock - :q WHERE article=:a", {"a": a, "q": q})
    else:
        raise ValueError("Type mouvement invalide")


def insert_movement(
    date_mvt: dt.date,
    article: str,
    designation: str,
    type_mvt: str,
    emplacement: str,
    quantite: int,
    adresse: str,
    commentaire: str,
) -> None:
    date_col = get_mvt_date_column_name()

    exec_sql(
        f"""
        INSERT INTO mouvements({date_col}, article, designation, type_mvt, emplacement, quantite, adresse, commentaire)
        VALUES (:date_mvt, :article, :designation, :type_mvt, :emplacement, :quantite, :adresse, :commentaire)
        """,
        {
            "date_mvt": date_mvt,
            "article": normalize_article(article),
            "designation": (designation or "").strip(),
            "type_mvt": type_mvt,
            "emplacement": emplacement,
            "quantite": int(quantite),
            "adresse": (adresse or "").strip(),
            "commentaire": (commentaire or "").strip(),
        },
    )


# ----------------------------
# ADRESSES Helpers
# ----------------------------
def add_address(nom: str) -> None:
    n = (nom or "").strip()
    if not n:
        return
    exec_sql(
        """
        INSERT INTO adresses(nom)
        VALUES (:n)
        ON CONFLICT (nom) DO NOTHING
        """,
        {"n": n},
    )


def delete_address(nom: str) -> None:
    n = (nom or "").strip()
    if not n:
        return
    exec_sql("DELETE FROM adresses WHERE nom=:n", {"n": n})


# ----------------------------
# UI: auto-remplissage designation
# ----------------------------
def on_article_change() -> None:
    a = normalize_article(st.session_state.get("mvt_article", ""))
    if not a:
        st.session_state["mvt_designation"] = ""
        return
    existing = get_article_designation(a)
    if existing:
        st.session_state["mvt_designation"] = existing
    # sinon on laisse vide -> l’utilisateur peut taper


# ============================================================
# UI
# ============================================================
st.title("📦 Gestion de stock")

tab_mvt, tab_stock, tab_addr = st.tabs(["➕ Mouvement", "📦 Stock actuel", "📍 Adresses"])

# ============================================================
# TAB 1 : MOUVEMENT
# ============================================================
with tab_mvt:
    st.subheader("Ajouter un mouvement")

    addr_df = read_df("SELECT nom FROM adresses ORDER BY nom")
    addr_list = [""] + addr_df["nom"].astype(str).tolist()

    with st.form("form_mvt", clear_on_submit=False):
        c1, c2, c3 = st.columns([1.2, 1.0, 1.3])

        with c1:
            date_mvt = st.date_input("Date", value=dt.date.today())
            st.text_input(
                "Numéro d'article",
                key="mvt_article",
                placeholder="Ex: 155082",
                on_change=on_article_change,
            )
            st.text_input("Désignation", key="mvt_designation", placeholder="Ex: Sonde O2")

        with c2:
            emplacement = st.selectbox("Emplacement", ["STOCK"], index=0)
            type_mvt = st.selectbox("Type", ["ENTREE", "SORTIE"], index=0)
            quantite = st.number_input("Quantité", min_value=1, max_value=10000, value=1, step=1)

            st.caption("Seuil pièce : 0 = aucun (seuil global utilisé)")
            seuil_piece = st.number_input("Seuil pièce (optionnel)", min_value=0, max_value=10000, value=0, step=1)
            maj_seuil = st.checkbox("Mettre à jour le seuil de cette pièce (même si elle existe déjà)", value=True)

        with c3:
            commentaire = st.text_area("Remarque / commentaire (optionnel)", height=120)
            adresse = st.selectbox("Adresse (optionnel)", addr_list, index=0)

        submitted = st.form_submit_button("✅ Enregistrer", use_container_width=True)

    if submitted:
        article = normalize_article(st.session_state.get("mvt_article", ""))
        designation = (st.session_state.get("mvt_designation") or "").strip()

        if not article:
            st.error("❌ Numéro d'article obligatoire.")
        else:
            existing_design = get_article_designation(article)

            # Si article existe, et designation vide -> on met celle de la DB
            if existing_design and not designation:
                designation = existing_design
                st.session_state["mvt_designation"] = existing_design

            # Si article inconnu ET designation vide -> erreur propre
            if (not existing_design) and (not designation):
                st.error("❌ Nouvel article : la désignation est obligatoire.")
            else:
                # Upsert + seuil
                if maj_seuil:
                    upsert_article(article, designation=designation, seuil_piece=int(seuil_piece))
                else:
                    upsert_article(article, designation=designation, seuil_piece=None)

                # Mouvement
                try:
                    apply_movement(article, type_mvt, int(quantite))
                    insert_movement(
                        date_mvt=date_mvt,
                        article=article,
                        designation=designation,
                        type_mvt=type_mvt,
                        emplacement=emplacement,
                        quantite=int(quantite),
                        adresse=adresse,
                        commentaire=commentaire,
                    )
                    cache_bust()
                    st.success("✅ Mouvement enregistré.")
                    st.rerun()
                except Exception as e:
                    st.exception(e)

    st.divider()

    # Modifier un article
    st.subheader("Modifier un article (désignation / seuil pièce / garantie)")

    articles_df = read_df(
        """
        SELECT article,
               COALESCE(designation,'') AS designation,
               COALESCE(seuil_piece,0) AS seuil_piece,
               COALESCE(garantie,0) AS garantie
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
                    max_value=10000,
                    value=int(row["seuil_piece"]),
                    step=1,
                )
                new_gar = st.number_input(
                    "Garantie (nombre)",
                    min_value=0,
                    max_value=10000,
                    value=int(row["garantie"]),
                    step=1,
                )
                save_edit = st.form_submit_button("💾 Enregistrer la modification", use_container_width=True)

            if save_edit:
                if not str(new_design or "").strip():
                    st.error("❌ La désignation ne peut pas être vide.")
                else:
                    try:
                        update_article_fields(
                            article=str(art_sel),
                            designation=str(new_design).strip(),
                            seuil_piece=int(new_seuil),
                            garantie=int(new_gar),
                        )
                        cache_bust()
                        st.success("✅ Article modifié.")
                        st.rerun()
                    except Exception as e:
                        st.exception(e)

# ============================================================
# TAB 2 : STOCK ACTUEL
# ============================================================
with tab_stock:
    st.subheader("Stock actuel")

    search = st.text_input("Recherche", placeholder="Numéro ou mot dans désignation...").strip().lower()

    # ✅ Stock SANS la colonne garantie
    df_stock = read_df(
        """
        SELECT article,
               COALESCE(designation,'') AS designation,
               stock,
               COALESCE(seuil_piece,0) AS seuil_piece
        FROM articles
        ORDER BY article
        """
    )

    if search:
        df_view = df_stock[
            df_stock["article"].astype(str).str.lower().str.contains(search, na=False)
            | df_stock["designation"].astype(str).str.lower().str.contains(search, na=False)
        ].copy()
    else:
        df_view = df_stock.copy()

    st.dataframe(df_view, use_container_width=True, height=360)
    st.divider()

    # Pièces à commander
    st.subheader("📦 Pièces à commander")
    seuil_global = get_setting_int("seuil_global", 0)

    colS1, colS2 = st.columns([1.2, 2.0])
    with colS1:
        new_global = st.number_input(
            "Seuil global (utilisé si seuil pièce = 0)",
            min_value=0,
            max_value=10000,
            value=int(seuil_global),
            step=1,
        )
        if st.button("💾 Sauver seuil global", use_container_width=True):
            set_setting_int("seuil_global", int(new_global))
            cache_bust()
            st.success("✅ Seuil global sauvegardé.")
            st.rerun()

    with colS2:
        df_order = df_stock.copy()
        df_order["seuil_utilise"] = df_order["seuil_piece"].apply(lambda x: int(x) if int(x) > 0 else int(seuil_global))
        df_order["a_commander"] = (df_order["seuil_utilise"] - df_order["stock"]).clip(lower=0)

        to_order = df_order[df_order["a_commander"] > 0][
            ["article", "designation", "stock", "seuil_piece", "seuil_utilise", "a_commander"]
        ].sort_values(["a_commander", "article"], ascending=[False, True])

        if to_order.empty:
            st.success("✅ Rien à commander.")
        else:
            st.dataframe(to_order, use_container_width=True, height=260)

    st.divider()

    # Historique (300)
    st.subheader("Historique (300 derniers)")
    date_col = get_mvt_date_column_name()
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
    st.dataframe(hist, use_container_width=True, height=420)

    st.divider()

    # Tableau garantie (séparé)
    st.subheader("🛡️ Tableau garantie")
    df_gar = read_df(
        """
        SELECT article,
               COALESCE(designation,'') AS designation,
               COALESCE(garantie,0) AS garantie
        FROM articles
        ORDER BY article
        """
    )
    st.dataframe(df_gar, use_container_width=True, height=320)

    st.divider()

    # Supprimer article (en bas)
    st.subheader("🗑️ Supprimer un article (tout en bas)")
    st.warning("⚠️ Supprime aussi les mouvements liés à cet article.")

    articles_df2 = read_df("SELECT article FROM articles ORDER BY article")
    articles_list2 = articles_df2["article"].astype(str).tolist()

    if not articles_list2:
        st.info("Aucun article à supprimer.")
    else:
        a_del = st.selectbox("Choisir l'article à supprimer", articles_list2, key="del_article_sel")
        confirm = st.checkbox("Je confirme la suppression (irréversible)", value=False)
        if st.button("🗑️ Supprimer définitivement", use_container_width=True, disabled=not confirm):
            try:
                delete_article(a_del)
                cache_bust()
                st.success("✅ Article supprimé.")
                st.rerun()
            except Exception as e:
                st.exception(e)

# ============================================================
# TAB 3 : ADRESSES
# ============================================================
with tab_addr:
    st.subheader("Adresses")

    left, right = st.columns([1.2, 1.2])

    with left:
        st.markdown("### ➕ Ajouter une adresse")
        with st.form("form_add_addr"):
            new_addr = st.text_input("Adresse", placeholder="Ex: Avenue de Miremont 27AB, 1206 Genève")
            add_btn = st.form_submit_button("✅ Ajouter", use_container_width=True)
        if add_btn:
            try:
                add_address(new_addr)
                cache_bust()
                st.success("✅ Adresse ajoutée (ou déjà existante).")
                st.rerun()
            except Exception as e:
                st.exception(e)

    with right:
        st.markdown("### 🗑️ Supprimer une adresse")
        addr_df2 = read_df("SELECT nom FROM adresses ORDER BY nom")
        addr_list2 = addr_df2["nom"].astype(str).tolist()

        if not addr_list2:
            st.info("Aucune adresse à supprimer.")
        else:
            addr_to_del = st.selectbox("Adresse à supprimer", addr_list2)
            confirm2 = st.checkbox("Je confirme la suppression", value=False, key="confirm_del_addr")
            if st.button("🗑️ Supprimer l'adresse", use_container_width=True, disabled=not confirm2):
                try:
                    delete_address(addr_to_del)
                    cache_bust()
                    st.success("✅ Adresse supprimée.")
                    st.rerun()
                except Exception as e:
                    st.exception(e)

    st.divider()
    st.markdown("### Liste des adresses")
    st.dataframe(read_df("SELECT nom FROM adresses ORDER BY nom"), use_container_width=True, height=360)