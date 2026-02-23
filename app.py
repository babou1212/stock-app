# app.py
from _future_ import annotations

import os
import datetime as dt
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ----------------------------
# CONFIG
# ----------------------------
st.set_page_config(page_title="Gestion de stock", layout="wide")

def get_database_url() -> str:
    # Streamlit Cloud -> st.secrets ; sinon env var
    if "DATABASE_URL" in st.secrets:
        return str(st.secrets["DATABASE_URL"]).strip()
    url = os.environ.get("DATABASE_URL", "").strip()
    return url

DB_URL = get_database_url()
if not DB_URL:
    st.error("❌ Variable d'environnement DATABASE_URL manquante.")
    st.stop()

# Si l’utilisateur met une URL postgresql://... (psycopg2 OK)
# On accepte aussi postgresql+psycopg2://...
ENGINE: Engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    future=True,
)

# ----------------------------
# DB HELPERS
# ----------------------------
def exec_sql(sql: str, params: Optional[dict[str, Any]] = None) -> None:
    params = params or {}
    with ENGINE.begin() as conn:
        conn.execute(text(sql), params)

def read_df(sql: str, params: Optional[dict[str, Any]] = None) -> pd.DataFrame:
    params = params or {}
    with ENGINE.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params)

def cache_bust() -> None:
    # simple cache bust: reset some cached dfs if you add caching later
    pass

# ----------------------------
# INIT DB (TABLES)
# ----------------------------
def init_db() -> None:
    # articles: stock + designation + garantie + seuil_piece
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS articles (
            article      TEXT PRIMARY KEY,
            designation  TEXT NOT NULL DEFAULT '',
            stock        INTEGER NOT NULL DEFAULT 0,
            garantie     INTEGER NOT NULL DEFAULT 0,
            seuil_piece  INTEGER NOT NULL DEFAULT 0
        );
        """
    )

    # mouvements: historique
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS mouvements (
            id           BIGSERIAL PRIMARY KEY,
            date_mvt     DATE NOT NULL,
            article      TEXT NOT NULL REFERENCES articles(article) ON DELETE CASCADE,
            designation  TEXT NOT NULL DEFAULT '',
            type_mvt     TEXT NOT NULL,
            emplacement  TEXT NOT NULL DEFAULT 'STOCK',
            quantite     INTEGER NOT NULL,
            adresse      TEXT NOT NULL DEFAULT '',
            commentaire  TEXT NOT NULL DEFAULT ''
        );
        """
    )

    # adresses: une seule colonne "adresse" (unique / not null)
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS adresses (
            id       SERIAL PRIMARY KEY,
            adresse  TEXT NOT NULL UNIQUE
        );
        """
    )

    # settings: k/v
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS settings (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        );
        """
    )

init_db()

# ----------------------------
# SETTINGS
# ----------------------------
def get_setting_int(key: str, default: int = 0) -> int:
    df = read_df("SELECT v FROM settings WHERE k=:k", {"k": key})
    if df.empty:
        return int(default)
    try:
        return int(df.iloc[0]["v"])
    except Exception:
        return int(default)

def set_setting_int(key: str, value: int) -> None:
    exec_sql(
        """
        INSERT INTO settings(k, v) VALUES (:k, :v)
        ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
        """,
        {"k": key, "v": str(int(value))}
    )

# ----------------------------
# ARTICLES HELPERS
# ----------------------------
def get_article(article: str) -> Optional[dict[str, Any]]:
    df = read_df(
        """
        SELECT article, designation, stock, garantie, seuil_piece
        FROM articles
        WHERE article = :a
        """,
        {"a": article},
    )
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    return row

def upsert_article(
    article: str,
    designation: Optional[str] = None,
    garantie: Optional[int] = None,
    seuil_piece: Optional[int] = None,
) -> None:
    # On crée l’article si absent.
    # On met à jour uniquement les champs fournis (sinon on garde ceux en base).
    existing = get_article(article)

    if existing is None:
        exec_sql(
            """
            INSERT INTO articles(article, designation, stock, garantie, seuil_piece)
            VALUES (:a, :d, 0, :g, :s)
            ON CONFLICT (article) DO NOTHING
            """,
            {
                "a": article,
                "d": (designation or "").strip(),
                "g": int(garantie) if garantie is not None else 0,
                "s": int(seuil_piece) if seuil_piece is not None else 0,
            },
        )
        return

    # Update partiel
    new_design = existing["designation"]
    if designation is not None:
        # si utilisateur donne vide, on accepte (mais en général on veut garder l’existant)
        # => ici: si designation == "" on laisse l'existant
        des = designation.strip()
        if des != "":
            new_design = des

    new_gar = int(existing["garantie"])
    if garantie is not None:
        new_gar = int(garantie)

    new_seuil = int(existing["seuil_piece"])
    if seuil_piece is not None:
        new_seuil = int(seuil_piece)

    exec_sql(
        """
        UPDATE articles
        SET designation = :d,
            garantie = :g,
            seuil_piece = :s
        WHERE article = :a
        """,
        {"a": article, "d": new_design, "g": new_gar, "s": new_seuil},
    )

def update_stock(article: str, delta: int) -> None:
    exec_sql(
        """
        UPDATE articles
        SET stock = stock + :d
        WHERE article = :a
        """,
        {"a": article, "d": int(delta)},
    )

# ----------------------------
# MOUVEMENTS
# ----------------------------
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
    exec_sql(
        """
        INSERT INTO mouvements(
            date_mvt, article, designation, type_mvt, emplacement, quantite, adresse, commentaire
        ) VALUES (
            :date_mvt, :article, :designation, :type_mvt, :emplacement, :quantite, :adresse, :commentaire
        )
        """,
        {
            "date_mvt": date_mvt,
            "article": article,
            "designation": (designation or "").strip(),
            "type_mvt": type_mvt,
            "emplacement": emplacement,
            "quantite": int(quantite),
            "adresse": (adresse or "").strip(),
            "commentaire": (commentaire or "").strip(),
        },
    )

def apply_movement(article: str, type_mvt: str, quantite: int) -> None:
    q = int(quantite)
    if type_mvt == "ENTREE":
        update_stock(article, +q)
    else:
        update_stock(article, -q)

# ----------------------------
# ADRESSES
# ----------------------------
def list_adresses() -> list[str]:
    df = read_df("SELECT adresse FROM adresses ORDER BY adresse")
    return df["adresse"].astype(str).tolist() if not df.empty else []

def add_adresse(adresse: str) -> None:
    a = (adresse or "").strip()
    if not a:
        raise ValueError("Adresse vide.")
    exec_sql(
        """
        INSERT INTO adresses(adresse) VALUES (:a)
        ON CONFLICT (adresse) DO NOTHING
        """,
        {"a": a},
    )

def delete_adresse(adresse: str) -> None:
    exec_sql("DELETE FROM adresses WHERE adresse = :a", {"a": adresse})

# ----------------------------
# UI HELPERS (auto designation)
# ----------------------------
def load_designation_from_article_key() -> None:
    article = (st.session_state.get("mvt_article", "") or "").strip()
    if not article:
        return
    row = get_article(article)
    if row and row.get("designation") is not None:
        st.session_state["mvt_designation"] = str(row["designation"])
    else:
        # pas trouvé: on ne met pas d'erreur, on laisse vide
        st.session_state["mvt_designation"] = ""

# ----------------------------
# UI
# ----------------------------
st.title("Gestion de stock")

tab_mvt, tab_stock, tab_addr = st.tabs(["Mouvements", "Stock actuel", "Adresses"])

# ============================
# TAB 1 : MOUVEMENTS
# ============================
with tab_mvt:
    st.subheader("Ajouter un mouvement")

    # IMPORTANT: champ article en dehors du form pour autoriser on_change
    c1, c2, c3 = st.columns([1.1, 1.1, 1.8])
    with c1:
        st.text_input(
            "Numéro d'article",
            key="mvt_article",
            placeholder="Ex: 155082",
            on_change=load_designation_from_article_key,
        )

    # Valeur par défaut si pas encore initialisée
    if "mvt_designation" not in st.session_state:
        st.session_state["mvt_designation"] = ""

    with st.form("form_mvt", clear_on_submit=False):
        colA, colB, colC = st.columns([1.1, 1.1, 1.8])

        with colA:
            date_mvt = st.date_input("Date", value=dt.date.today(), key="mvt_date")
            # designation auto-remplie si article existe
            designation = st.text_input(
                "Désignation (auto si article existant)",
                key="mvt_designation",
                placeholder="Ex: Sonde O2",
            )

        with colB:
            emplacement = st.selectbox("Emplacement", ["STOCK"], index=0, key="mvt_emplacement")
            type_mvt = st.selectbox("Type", ["ENTREE", "SORTIE"], index=0, key="mvt_type")
            quantite = st.number_input("Quantité", min_value=1, max_value=10_000, value=1, step=1, key="mvt_qte")

            st.caption("Seuil pièce : 0 = aucun (seuil global utilisé)")
            seuil_piece = st.number_input(
                "Seuil pièce (optionnel)",
                min_value=0, max_value=10_000, value=0, step=1, key="mvt_seuil_piece"
            )
            maj_seuil = st.checkbox(
                "Mettre à jour le seuil de cette pièce (même si elle existe déjà)",
                value=True,
                key="mvt_maj_seuil",
            )

        with colC:
            adrs = [""] + list_adresses()
            adresse_sel = st.selectbox("Adresse (optionnel)", adrs, index=0, key="mvt_adresse")
            commentaire = st.text_area("Remarque / commentaire (optionnel)", height=120, key="mvt_commentaire")

        submitted = st.form_submit_button("✅ Enregistrer", use_container_width=True)

    if submitted:
        article = (st.session_state.get("mvt_article", "") or "").strip()
        designation = (st.session_state.get("mvt_designation", "") or "").strip()

        if not article:
            st.error("❌ Numéro d'article obligatoire.")
        else:
            # Récup article si existe (pour récupérer désignation si user vide)
            existing = get_article(article)

            # Si designation est vide et existe en base -> on prend celle en base
            if (designation == "") and existing and str(existing.get("designation", "")).strip() != "":
                designation = str(existing["designation"]).strip()

            # Si designation reste vide -> pas d'erreur, on accepte (tu as dit que ça ne te dérange pas)
            # Création / MAJ article
            if maj_seuil:
                upsert_article(
                    article=article,
                    designation=designation if designation != "" else None,  # si vide, on ne force pas
                    seuil_piece=int(seuil_piece),
                )
            else:
                upsert_article(
                    article=article,
                    designation=designation if designation != "" else None,
                )

            # Appliquer mouvement + log
            apply_movement(article, type_mvt, int(quantite))
            insert_mouvement(
                date_mvt=date_mvt,
                article=article,
                designation=designation,
                type_mvt=type_mvt,
                emplacement=emplacement,
                quantite=int(quantite),
                adresse=adresse_sel or "",
                commentaire=commentaire or "",
            )

            st.success("✅ Mouvement enregistré.")
            st.rerun()

    st.divider()
    st.subheader("Modifier un article (désignation / seuil pièce / garantie)")

    articles_df = read_df(
        """
        SELECT article, designation, garantie, COALESCE(seuil_piece,0) AS seuil_piece
        FROM articles
        ORDER BY article
        """
    )
    articles_list = articles_df["article"].astype(str).tolist() if not articles_df.empty else []

    if not articles_list:
        st.info("Aucun article à modifier.")
    else:
        colL, colR = st.columns([1.2, 1.8])
        with colL:
            art_sel = st.selectbox("Choisir l'article", articles_list, key="edit_article_sel")
            row = articles_df[articles_df["article"].astype(str) == str(art_sel)].iloc[0]

        with colR:
            with st.form("form_edit_article"):
                new_design = st.text_input("Désignation", value=str(row["designation"] or ""))
                new_seuil = st.number_input(
                    "Seuil pièce (0 = pas de seuil perso)",
                    min_value=0, max_value=10_000, value=int(row["seuil_piece"]), step=1
                )
                new_gar = st.number_input(
                    "Garantie (nombre)",
                    min_value=0, max_value=10_000, value=int(row["garantie"]), step=1
                )
                save_edit = st.form_submit_button("💾 Enregistrer la modification", use_container_width=True)

            if save_edit:
                upsert_article(
                    article=str(art_sel),
                    designation=new_design,  # si vide => on garde l'existant (voir upsert_article)
                    garantie=int(new_gar),
                    seuil_piece=int(new_seuil),
                )
                st.success("✅ Article modifié.")
                st.rerun()

# ============================
# TAB 2 : STOCK
# ============================
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

    if not df.empty and search:
        df_view = df[
            df["article"].astype(str).str.lower().str.contains(search, na=False)
            | df["designation"].astype(str).str.lower().str.contains(search, na=False)
        ].copy()
    else:
        df_view = df.copy()

    st.dataframe(df_view, use_container_width=True, height=360)

    st.divider()

    # PIECES A COMMANDER
    st.subheader("Pièces à commander")
    seuil_global = st.number_input(
        "Seuil global (utilisé si seuil pièce = 0)",
        min_value=0, max_value=10_000,
        value=int(get_setting_int("seuil_global", 0)),
        step=1,
        key="seuil_global_input",
    )
    if st.button("💾 Sauvegarder le seuil global", use_container_width=True):
        set_setting_int("seuil_global", int(seuil_global))
        st.success("✅ Seuil global sauvegardé.")
        st.rerun()

    if not df.empty:
        # seuil_effectif = seuil_piece si >0 sinon seuil_global
        df_calc = df.copy()
        df_calc["seuil_effectif"] = df_calc["seuil_piece"].apply(lambda x: int(x) if int(x) > 0 else int(seuil_global))
        to_order = df_calc[df_calc["stock"].astype(int) < df_calc["seuil_effectif"].astype(int)].copy()
        if to_order.empty:
            st.success("✅ Rien à commander.")
        else:
            to_order["a_commander"] = to_order["seuil_effectif"].astype(int) - to_order["stock"].astype(int)
            st.dataframe(
                to_order[["article", "designation", "stock", "seuil_piece", "seuil_effectif", "a_commander"]],
                use_container_width=True,
                height=260,
            )

    st.divider()

    # GARANTIES (table séparée)
    st.subheader("Garanties")
    gar_df = read_df(
        """
        SELECT article, designation, garantie
        FROM articles
        WHERE COALESCE(garantie,0) > 0
        ORDER BY article
        """
    )
    if gar_df.empty:
        st.info("Aucune garantie renseignée.")
    else:
        st.dataframe(gar_df, use_container_width=True, height=220)

    st.divider()

    # HISTORIQUE
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

    st.divider()

    # SUPPRIMER UN ARTICLE
    st.subheader("🗑️ Supprimer un article (tout en bas)")
    st.warning("⚠️ Supprime aussi les mouvements liés à cet article (cascade).")

    articles_df2 = read_df("SELECT article FROM articles ORDER BY article")
    articles_list2 = articles_df2["article"].astype(str).tolist() if not articles_df2.empty else []

    if len(articles_list2) == 0:
        st.info("Aucun article à supprimer.")
    else:
        del_article = st.selectbox("Article à supprimer", articles_list2, key="del_article_sel")
        confirm = st.checkbox("Je confirme la suppression définitive", value=False, key="del_article_confirm")
        if st.button("❌ Supprimer définitivement", use_container_width=True, disabled=not confirm):
            exec_sql("DELETE FROM articles WHERE article=:a", {"a": str(del_article)})
            st.success("✅ Article supprimé.")
            st.rerun()

# ============================
# TAB 3 : ADRESSES
# ============================
with tab_addr:
    st.subheader("Adresses")

    st.markdown("### Ajouter une adresse")
    with st.form("form_add_adresse"):
        new_addr = st.text_input("Nouvelle adresse", placeholder="Ex: Avenue de Miremont 27A/B, 1206 Genève")
        add_btn = st.form_submit_button("✅ Ajouter", use_container_width=True)
    if add_btn:
        try:
            add_adresse(new_addr)
            st.success("✅ Adresse ajoutée (ou déjà existante).")
            st.rerun()
        except ValueError:
            st.error("❌ Adresse vide.")
        except Exception as e:
            st.exception(e)

    st.divider()

    st.markdown("### Supprimer une adresse")
    addr_list = list_adresses()
    if not addr_list:
        st.info("Aucune adresse enregistrée.")
    else:
        addr_to_del = st.selectbox("Adresse à supprimer", addr_list, key="addr_to_del")
        confirm_addr = st.checkbox("Je confirme la suppression", value=False, key="confirm_addr_del")
        if st.button("🗑️ Supprimer l'adresse", use_container_width=True, disabled=not confirm_addr):
            delete_adresse(addr_to_del)
            st.success("✅ Adresse supprimée.")
            st.rerun()
