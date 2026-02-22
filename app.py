# app.py
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# =========================
# CONFIG
# =========================
st.set_page_config(page_title="Gestion de stock", page_icon="📦", layout="wide")

DEFAULT_SEUIL_GLOBAL = 3
HIST_LIMIT = 300

# =========================
# DB
# =========================
def get_engine() -> Engine:
    db_url = st.secrets.get("DB_URL", "").strip()
    if not db_url:
        st.error("❌ DB_URL manquant dans Secrets (Streamlit).")
        st.stop()

    # pool_pre_ping évite les connexions mortes, pool_recycle évite les timeouts long
    return create_engine(db_url, pool_pre_ping=True, pool_recycle=1800)


ENGINE = get_engine()


def exec_sql(sql: str, params: Optional[dict] = None) -> None:
    params = params or {}
    with ENGINE.begin() as conn:
        conn.execute(text(sql), params)


@st.cache_data(ttl=10, show_spinner=False)
def read_df(sql: str, params: Optional[dict] = None) -> pd.DataFrame:
    params = params or {}
    with ENGINE.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def cache_bust() -> None:
    try:
        st.cache_data.clear()
    except Exception:
        pass


def init_db() -> None:
    # Tables principales
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
            designation TEXT NOT NULL DEFAULT '',
            stock INTEGER NOT NULL DEFAULT 0,
            garantie INTEGER NOT NULL DEFAULT 0,
            seuil_piece INTEGER NOT NULL DEFAULT 0
        );
        """
    )

    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS adresses (
            id SERIAL PRIMARY KEY,
            nom TEXT UNIQUE NOT NULL
        );
        """
    )

    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS mouvements (
            id BIGSERIAL PRIMARY KEY,
            date_mvt DATE NOT NULL,
            article TEXT NOT NULL,
            designation TEXT NOT NULL DEFAULT '',
            type_mvt TEXT NOT NULL,
            emplacement TEXT NOT NULL DEFAULT 'STOCK',
            quantite INTEGER NOT NULL,
            adresse TEXT NOT NULL DEFAULT '',
            commentaire TEXT NOT NULL DEFAULT ''
        );
        """
    )

    # “Migrations” souples (si tu avais une DB déjà existante)
    for ddl in [
        "ALTER TABLE articles ADD COLUMN IF NOT EXISTS garantie INTEGER NOT NULL DEFAULT 0;",
        "ALTER TABLE articles ADD COLUMN IF NOT EXISTS seuil_piece INTEGER NOT NULL DEFAULT 0;",
        "ALTER TABLE mouvements ADD COLUMN IF NOT EXISTS adresse TEXT NOT NULL DEFAULT '';",
        "ALTER TABLE mouvements ADD COLUMN IF NOT EXISTS commentaire TEXT NOT NULL DEFAULT '';",
    ]:
        try:
            exec_sql(ddl)
        except Exception:
            # si une DB refuse un IF NOT EXISTS selon config, on ignore
            pass

    # Index (optionnel, améliore les perfs)
    for idx in [
        "CREATE INDEX IF NOT EXISTS idx_mouvements_article ON mouvements(article);",
        "CREATE INDEX IF NOT EXISTS idx_mouvements_date ON mouvements(date_mvt);",
    ]:
        try:
            exec_sql(idx)
        except Exception:
            pass

    # Valeur par défaut du seuil global si absent
    if get_setting_int("seuil_global", DEFAULT_SEUIL_GLOBAL) is None:
        set_setting("seuil_global", str(DEFAULT_SEUIL_GLOBAL))


def get_setting(key: str, default: str) -> str:
    df = read_df("SELECT value FROM settings WHERE key=:k", {"k": key})
    if df.empty:
        return default
    return str(df.iloc[0]["value"])


def get_setting_int(key: str, default: int) -> Optional[int]:
    try:
        val = get_setting(key, str(default))
        return int(val)
    except Exception:
        return default


def set_setting(key: str, value: str) -> None:
    exec_sql(
        """
        INSERT INTO settings(key, value)
        VALUES (:k, :v)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
        """,
        {"k": key, "v": value},
    )


# =========================
# LOGIQUE METIER
# =========================
@dataclass
class ArticleEdit:
    article: str
    designation: str
    garantie: int
    seuil_piece: int


def upsert_article(article: str, designation: str, garantie: int = 0, seuil_piece: int = 0) -> None:
    exec_sql(
        """
        INSERT INTO articles(article, designation, stock, garantie, seuil_piece)
        VALUES (:a, :d, 0, :g, :s)
        ON CONFLICT (article) DO UPDATE SET
            designation = EXCLUDED.designation,
            garantie = EXCLUDED.garantie,
            seuil_piece = EXCLUDED.seuil_piece;
        """,
        {"a": article, "d": designation, "g": int(garantie), "s": int(seuil_piece)},
    )


def update_article_fields(edit: ArticleEdit) -> None:
    exec_sql(
        """
        UPDATE articles
        SET designation = :d,
            garantie = :g,
            seuil_piece = :s
        WHERE article = :a;
        """,
        {"a": edit.article, "d": edit.designation, "g": int(edit.garantie), "s": int(edit.seuil_piece)},
    )


def apply_movement(article: str, designation: str, type_mvt: str, quantite: int) -> None:
    # Met à jour le stock dans articles
    delta = int(quantite)
    if type_mvt.upper() == "SORTIE":
        delta = -abs(delta)
    else:
        delta = abs(delta)

    exec_sql(
        """
        INSERT INTO articles(article, designation, stock, garantie, seuil_piece)
        VALUES (:a, :d, 0, 0, 0)
        ON CONFLICT (article) DO UPDATE SET
            designation = CASE
                WHEN EXCLUDED.designation <> '' THEN EXCLUDED.designation
                ELSE articles.designation
            END;
        """,
        {"a": article, "d": designation},
    )

    exec_sql(
        """
        UPDATE articles
        SET stock = stock + :delta
        WHERE article = :a;
        """,
        {"a": article, "delta": int(delta)},
    )


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
    exec_sql(
        """
        INSERT INTO mouvements(date_mvt, article, designation, type_mvt, emplacement, quantite, adresse, commentaire)
        VALUES (:date_mvt, :article, :designation, :type_mvt, :emplacement, :quantite, :adresse, :commentaire);
        """,
        {
            "date_mvt": date_mvt,
            "article": article,
            "designation": designation,
            "type_mvt": type_mvt,
            "emplacement": emplacement,
            "quantite": int(quantite),
            "adresse": adresse or "",
            "commentaire": commentaire or "",
        },
    )


def delete_article(article: str) -> None:
    # Supprime aussi les mouvements liés
    exec_sql("DELETE FROM mouvements WHERE article = :a", {"a": article})
    exec_sql("DELETE FROM articles WHERE article = :a", {"a": article})


# =========================
# UI
# =========================
init_db()

st.title("📦 Gestion de stock")

tab_mvt, tab_stock, tab_addr = st.tabs(["➕ Mouvement", "📦 Stock actuel", "📍 Adresses"])


# -------------------------
# TAB 1 : MOUVEMENT + MODIF ARTICLE
# -------------------------
with tab_mvt:
    st.subheader("Ajouter un mouvement")

    # Charge adresses
    addr_df = read_df("SELECT nom FROM adresses ORDER BY nom")
    addr_list = [""] + addr_df["nom"].tolist()

    with st.form("form_mvt", clear_on_submit=False):
        col1, col2, col3 = st.columns([1.1, 1.1, 1.8])

        with col1:
            date_mvt = st.date_input("Date", value=dt.date.today())
            article = st.text_input("Numéro d'article", placeholder="Ex: 155082").strip()
            designation = st.text_input("Désignation", placeholder="Ex: Sonde O2").strip()

        with col2:
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

        with col3:
            commentaire = st.text_area("Remarque / commentaire (optionnel)", height=120)
            adresse = st.selectbox("Adresse (optionnel)", addr_list, index=0)

            submitted = st.form_submit_button("✅ Enregistrer", use_container_width=True)

    if submitted:
        if not article:
            st.error("❌ Numéro d'article obligatoire.")
        else:
            # crée/maj article (désignation + éventuellement seuil)
            if maj_seuil:
                # Si designation vide, on garde ce qu’on a en DB si existe
                if designation:
                    upsert_article(article, designation, garantie=0, seuil_piece=int(seuil_piece))
                else:
                    # si designation vide, on met juste le seuil
                    exec_sql(
                        """
                        INSERT INTO articles(article, designation, stock, garantie, seuil_piece)
                        VALUES (:a, '', 0, 0, :s)
                        ON CONFLICT (article) DO UPDATE SET seuil_piece = :s;
                        """,
                        {"a": article, "s": int(seuil_piece)},
                    )
            else:
                if designation:
                    upsert_article(article, designation, garantie=0, seuil_piece=0)

            # applique mouvement + log mouvement
            apply_movement(article, designation, type_mvt, int(quantite))
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

    st.divider()

    st.subheader("Modifier un article (désignation / garantie / seuil pièce)")

    articles_df = read_df(
        """
        SELECT article, designation, garantie, COALESCE(seuil_piece,0) AS seuil_piece
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
                new_gar = st.number_input("Garantie", min_value=0, max_value=10_000, value=int(row["garantie"]), step=1)
                new_seuil = st.number_input(
                    "Seuil pièce (0 = pas de seuil perso)",
                    min_value=0,
                    max_value=10_000,
                    value=int(row["seuil_piece"]),
                    step=1,
                )
                save_edit = st.form_submit_button("💾 Enregistrer la modification", use_container_width=True)

            if save_edit:
                update_article_fields(
                    ArticleEdit(
                        article=str(art_sel),
                        designation=new_design.strip(),
                        garantie=int(new_gar),
                        seuil_piece=int(new_seuil),
                    )
                )
                cache_bust()
                st.success("✅ Article modifié.")
                st.rerun()


# -------------------------
# TAB 2 : STOCK ACTUEL + PIECES A COMMANDER + HISTORIQUE + SUPPRIMER (EN BAS)
# -------------------------
with tab_stock:
    st.subheader("Stock actuel")

    search = st.text_input("Recherche", placeholder="Numéro ou mot dans désignation…").strip().lower()

    df = read_df(
        """
        SELECT article, designation, stock, COALESCE(seuil_piece,0) AS seuil_piece
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

    st.subheader("📦 Pièces à commander")

    # Seuil global
    seuil_global_db = get_setting_int("seuil_global", DEFAULT_SEUIL_GLOBAL)
    col1, col2 = st.columns([1.2, 2.8])

    with col1:
        seuil_global = st.number_input(
            "Seuil global (utilisé si seuil pièce = 0)",
            min_value=0,
            max_value=10_000,
            value=int(seuil_global_db),
            step=1,
        )
        if seuil_global != seuil_global_db:
            set_setting("seuil_global", str(int(seuil_global)))
            cache_bust()
            st.success("✅ Seuil global mis à jour.")
            st.rerun()

    with col2:
        # Calcul : seuil utilisé = seuil_piece si > 0 sinon seuil_global
        tmp = df.copy()
        tmp["seuil_utilise"] = tmp["seuil_piece"].apply(lambda x: int(x) if int(x) > 0 else int(seuil_global))
        a_commander = tmp[tmp["stock"].astype(int) <= tmp["seuil_utilise"].astype(int)].copy()
        a_commander = a_commander.sort_values(["stock", "designation", "article"], ascending=[True, True, True])

        if a_commander.empty:
            st.success("✅ Rien à commander.")
        else:
            st.dataframe(
                a_commander[["article", "designation", "stock", "seuil_piece", "seuil_utilise"]],
                use_container_width=True,
                height=300,
            )
            st.divider()

            st.subheader("🛡️ Tableau des garanties")

garantie_df = read_df(
    """
    SELECT article, designation, garantie
    FROM articles
    WHERE garantie > 0
    ORDER BY garantie DESC, article
    """
)

if garantie_df.empty:
    st.info("Aucune pièce avec garantie définie.")
else:
    st.dataframe(garantie_df, use_container_width=True, height=300)
    st.divider()

    st.subheader(f"Historique ({HIST_LIMIT} derniers)")

    hist = read_df(
        """
        SELECT id, date_mvt, article, designation, type_mvt, emplacement, quantite,
               COALESCE(adresse,'') AS adresse,
               COALESCE(commentaire,'') AS commentaire
        FROM mouvements
        ORDER BY id DESC
        LIMIT :lim
        """,
        {"lim": HIST_LIMIT},
    )
    st.dataframe(hist, use_container_width=True, height=420)

    st.divider()

    st.subheader("🗑️ Supprimer un article (tout en bas)")
    st.warning("⚠️ Supprime aussi tous les mouvements liés à cet article.")

    articles_df2 = read_df("SELECT article FROM articles ORDER BY article")
    articles_list2 = articles_df2["article"].astype(str).tolist()

    if not articles_list2:
        st.info("Aucun article à supprimer.")
    else:
        a_del = st.selectbox("Choisir l'article à supprimer", articles_list2, key="del_article_sel")
        confirm = st.checkbox("Je confirme la suppression (irréversible)", value=False)

        if st.button("❌ Supprimer définitivement", use_container_width=True, disabled=not confirm):
            try:
                delete_article(str(a_del))
                cache_bust()
                st.success("✅ Article supprimé.")
                st.rerun()
            except Exception as e:
                st.exception(e)


# -------------------------
# TAB 3 : ADRESSES
# -------------------------
with tab_addr:
    st.subheader("Adresses")

    colL, colR = st.columns([1.3, 1.7])

    with colL:
        st.write("Ajouter une adresse")
        with st.form("form_add_addr", clear_on_submit=True):
            nom = st.text_input("Nom", placeholder="Ex: Client Dupont / Chantier X / etc.").strip()
            ok = st.form_submit_button("➕ Ajouter", use_container_width=True)
        if ok:
            if not nom:
                st.error("❌ Nom obligatoire.")
            else:
                try:
                    exec_sql("INSERT INTO adresses(nom) VALUES (:n)", {"n": nom})
                    cache_bust()
                    st.success("✅ Adresse ajoutée.")
                    st.rerun()
                except Exception as e:
                    st.error("❌ Adresse déjà existante ou erreur DB.")
                    st.exception(e)

    with colR:
        st.write("Liste / suppression")
        addr = read_df("SELECT nom FROM adresses ORDER BY nom")
        st.dataframe(addr, use_container_width=True, height=260)

        if not addr.empty:
            a = st.selectbox("Adresse à supprimer", addr["nom"].tolist(), key="addr_del_sel")
            if st.button("🗑️ Supprimer l'adresse", use_container_width=True):
                try:
                    exec_sql("DELETE FROM adresses WHERE nom=:n", {"n": a})
                    cache_bust()
                    st.success("✅ Adresse supprimée.")
                    st.rerun()
                except Exception as e:
                    st.exception(e)
        else:
            st.info("Aucune adresse enregistrée.")


