from __future__ import annotations

import os
from datetime import date
from typing import Optional, Dict, Any, List

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# -----------------------------
# CONFIG
# -----------------------------
APP_TITLE = "Gestion de stock"
DEFAULT_GLOBAL_SEUIL = 3  # utilisé si un article n'a pas de seuil propre

st.set_page_config(page_title=APP_TITLE, layout="wide")

# -----------------------------
# DB
# -----------------------------
def get_engine() -> Engine:
    """
    DB_URL doit être défini dans Streamlit Secrets (TOML) :
    DB_URL="postgresql+psycopg2://user:password@host:5432/dbname"
    """
    db_url = st.secrets.get("DB_URL", "").strip()
    if not db_url:
        st.error("DB_URL manquant. Ajoute DB_URL dans les Secrets Streamlit.")
        st.stop()

    # pool_pre_ping évite les connexions mortes (Supabase/Streamlit Cloud)
    return create_engine(db_url, pool_pre_ping=True)


ENGINE = get_engine()


def exec_sql(sql: str, params: Optional[Dict[str, Any]] = None) -> None:
    with ENGINE.begin() as conn:
        conn.execute(text(sql), params or {})


def read_df(sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    with ENGINE.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def init_db() -> None:
    """
    Crée les tables si besoin + ajoute les colonnes manquantes (migrations simples).
    """
    # Tables principales
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS articles (
            article TEXT PRIMARY KEY,
            designation TEXT NOT NULL,
            stock INTEGER NOT NULL DEFAULT 0,
            garantie INTEGER NOT NULL DEFAULT 0,
            seuil_commande INTEGER NULL
        );
        """
    )

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
            adresse TEXT NULL,
            commentaire TEXT NULL
        );
        """
    )

    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS adresses (
            id BIGSERIAL PRIMARY KEY,
            nom TEXT UNIQUE NOT NULL
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

    # Migrations "douces" : si tu avais une ancienne version
    exec_sql("ALTER TABLE articles ADD COLUMN IF NOT EXISTS seuil_commande INTEGER NULL;")
    exec_sql("ALTER TABLE mouvements ADD COLUMN IF NOT EXISTS adresse TEXT NULL;")
    exec_sql("ALTER TABLE mouvements ADD COLUMN IF NOT EXISTS commentaire TEXT NULL;")

    # Index utiles (perf)
    exec_sql("CREATE INDEX IF NOT EXISTS idx_mouvements_article ON mouvements(article);")
    exec_sql("CREATE INDEX IF NOT EXISTS idx_mouvements_date ON mouvements(date_mvt);")
    exec_sql("CREATE INDEX IF NOT EXISTS idx_articles_designation ON articles(designation);")


init_db()

# -----------------------------
# SETTINGS (global seuil)
# -----------------------------
def get_setting(key: str, default: str) -> str:
    df = read_df("SELECT v FROM settings WHERE k = :k", {"k": key})
    if df.empty:
        return default
    return str(df.iloc[0]["v"])


def set_setting(key: str, value: str) -> None:
    exec_sql(
        """
        INSERT INTO settings(k, v) VALUES (:k, :v)
        ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v
        """,
        {"k": key, "v": value},
    )


# -----------------------------
# CACHE (pour accélérer)
# -----------------------------
def clear_cache() -> None:
    st.cache_data.clear()


@st.cache_data(ttl=10)
def load_articles() -> pd.DataFrame:
    return read_df(
        """
        SELECT article, designation, stock, garantie, seuil_commande
        FROM articles
        ORDER BY article
        """
    )


@st.cache_data(ttl=10)
def load_adresses() -> List[str]:
    df = read_df("SELECT nom FROM adresses ORDER BY nom")
    return df["nom"].tolist() if not df.empty else []


@st.cache_data(ttl=10)
def load_historique(limit: int = 300) -> pd.DataFrame:
    return read_df(
        """
        SELECT id, date_mvt, article, designation, type_mvt, emplacement, quantite, adresse, commentaire
        FROM mouvements
        ORDER BY id DESC
        LIMIT :lim
        """,
        {"lim": int(limit)},
    )


def get_designation(article: str) -> str:
    df = read_df("SELECT designation FROM articles WHERE article = :a", {"a": article})
    return "" if df.empty else str(df.iloc[0]["designation"])


def upsert_article(article: str, designation: str, seuil_commande: Optional[int] = None) -> None:
    exec_sql(
        """
        INSERT INTO articles(article, designation, seuil_commande)
        VALUES (:a, :d, :s)
        ON CONFLICT (article)
        DO UPDATE SET designation = EXCLUDED.designation,
                      seuil_commande = EXCLUDED.seuil_commande
        """,
        {"a": str(article), "d": str(designation), "s": seuil_commande},
    )


def apply_stock_movement(article: str, emplacement: str, type_mvt: str, qty: int) -> None:
    """
    Met à jour stock/garantie selon emplacement + type.
    Ici on garde : STOCK et GARANTIE (mais sans boutons transfert dédiés).
    """
    qty = int(qty)
    if qty <= 0:
        raise ValueError("Quantité invalide.")

    if emplacement == "STOCK":
        if type_mvt == "ENTREE":
            exec_sql("UPDATE articles SET stock = stock + :q WHERE article = :a", {"q": qty, "a": article})
        else:
            exec_sql("UPDATE articles SET stock = GREATEST(stock - :q, 0) WHERE article = :a", {"q": qty, "a": article})

    elif emplacement == "GARANTIE":
        if type_mvt == "ENTREE":
            exec_sql("UPDATE articles SET garantie = garantie + :q WHERE article = :a", {"q": qty, "a": article})
        else:
            exec_sql("UPDATE articles SET garantie = GREATEST(garantie - :q, 0) WHERE article = :a", {"q": qty, "a": article})
    else:
        raise ValueError("Emplacement inconnu.")


def add_mouvement(
    d: date,
    article: str,
    designation: str,
    type_mvt: str,
    emplacement: str,
    qty: int,
    adresse: Optional[str],
    commentaire: Optional[str],
) -> None:
    exec_sql(
        """
        INSERT INTO mouvements(date_mvt, article, designation, type_mvt, emplacement, quantite, adresse, commentaire)
        VALUES (:dt, :a, :d, :t, :e, :q, :ad, :c)
        """,
        {
            "dt": d,
            "a": str(article),
            "d": str(designation),
            "t": str(type_mvt),
            "e": str(emplacement),
            "q": int(qty),
            "ad": (adresse if adresse else None),
            "c": (commentaire.strip() if commentaire and commentaire.strip() else None),
        },
    )


def delete_article(article: str) -> None:
    exec_sql("DELETE FROM mouvements WHERE article = :a", {"a": article})
    exec_sql("DELETE FROM articles WHERE article = :a", {"a": article})


# -----------------------------
# UI Helpers
# -----------------------------
def style_comment_red(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    return "color: #d00000; font-weight: 700;"


def with_comment_icon(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    if not s:
        return ""
    return f"📝 {s}"


# -----------------------------
# APP
# -----------------------------
st.title("📦 Gestion de stock")

tab_mvt, tab_stock, tab_addr = st.tabs(["➕ Mouvement", "📋 Stock / Gestion", "📍 Adresses"])

# =========================================
# TAB 1 : MOUVEMENT
# =========================================
with tab_mvt:
    st.subheader("Ajouter un mouvement")

    articles_df = load_articles()

    col1, col2, col3 = st.columns([1.2, 1.2, 1.6])

    with col1:
        d = st.date_input("Date", value=date.today())
        article_in = st.text_input("Numéro d'article", placeholder="Ex: 155082").strip()

        # si article existe : auto designation
        designation_auto = get_designation(article_in) if article_in else ""
        if designation_auto:
            st.caption(f"✅ Désignation (auto) : **{designation_auto}**")

        # NOUVEAU : seuil par pièce au moment où tu crées/modifies une pièce
        # - si l'article existe : on propose la valeur actuelle
        # - sinon : vide -> utilisera le seuil global
        current_seuil = None
        if article_in:
            row = articles_df[articles_df["article"].astype(str) == str(article_in)]
            if not row.empty:
                current_seuil = row.iloc[0]["seuil_commande"]
                if pd.isna(current_seuil):
                    current_seuil = None

        seuil_piece = st.number_input(
            "Seuil de cette pièce (optionnel)",
            min_value=0,
            max_value=10000,
            value=int(current_seuil) if current_seuil is not None else 0,
            step=1,
            help="Si tu mets 0 et que tu veux 'aucun seuil pièce', laisse plutôt vide via la case ci-dessous.",
        )
        use_seuil_piece = st.checkbox(
            "✅ Utiliser ce seuil pour cette pièce",
            value=True if current_seuil is not None else False,
            help="Coche pour enregistrer un seuil spécifique. Décoche pour revenir au seuil global.",
        )

        designation_first = ""
        if article_in and not designation_auto:
            designation_first = st.text_input("Désignation (1ère fois)", placeholder="Ex: Sonde O2").strip()

    with col2:
        emplacement = st.selectbox("Emplacement", ["STOCK", "GARANTIE"])
        type_mvt = st.selectbox("Type", ["ENTREE", "SORTIE"])
        qty = st.number_input("Quantité", min_value=1, max_value=100000, value=1, step=1)

        # Adresse destination (optionnel) -> PAS de ligne "Rechercher adresse..."
        adresses = load_adresses()
        addr_choices = ["(Aucune)"] + adresses + ["➕ Nouvelle adresse"]
        addr_pick = st.selectbox("Adresse destination (optionnel)", addr_choices)

        new_addr = ""
        if addr_pick == "➕ Nouvelle adresse":
            new_addr = st.text_input("Nouvelle adresse destination", placeholder="Ex: Chantier A, Client X, Atelier...").strip()

    with col3:
        commentaire = st.text_area("Remarque / commentaire (optionnel)", height=120)
        st.caption("ℹ️ Si tu mets une remarque, elle s’affichera en **rouge** dans l’historique.")

        btn = st.button("✅ Enregistrer", use_container_width=True)

    if btn:
        if not article_in:
            st.error("Tu dois entrer un numéro d'article.")
            st.stop()

        article = str(article_in)

        # adresse finale
        adresse_finale = None
        if addr_pick == "(Aucune)":
            adresse_finale = None
        elif addr_pick == "➕ Nouvelle adresse":
            if new_addr:
                # insert adresse si nouvelle
                try:
                    exec_sql(
                        "INSERT INTO adresses(nom) VALUES (:n) ON CONFLICT (nom) DO NOTHING",
                        {"n": new_addr},
                    )
                except Exception:
                    pass
                adresse_finale = new_addr
                clear_cache()
            else:
                adresse_finale = None
        else:
            adresse_finale = addr_pick

        # designation
        designation = designation_auto if designation_auto else designation_first
        if not designation:
            st.error("Cet article n'existe pas encore : tu dois saisir une désignation.")
            st.stop()

        # seuil par pièce
        seuil_to_save: Optional[int] = None
        if use_seuil_piece:
            seuil_to_save = int(seuil_piece)

        try:
            upsert_article(article, designation, seuil_to_save)
            apply_stock_movement(article, emplacement, type_mvt, int(qty))
            add_mouvement(d, article, designation, type_mvt, emplacement, int(qty), adresse_finale, commentaire)
            clear_cache()
            st.success("✅ Mouvement enregistré.")
        except Exception as e:
            st.error(f"Erreur : {e}")

# =========================================
# TAB 2 : STOCK / GESTION
# =========================================
with tab_stock:
    st.subheader("Stock actuel")

    df = load_articles().copy()

    # Barre de recherche (au-dessus)
    search = st.text_input("🔎 Rechercher un article (numéro)", placeholder="Tape un numéro (ex: 155082)")

    if search.strip():
        df_search = df[df["article"].astype(str).str.contains(search.strip(), case=False, na=False)].copy()
        st.caption("Résultat :")
        st.dataframe(df_search[["article", "designation", "stock", "garantie", "seuil_commande"]], use_container_width=True, height=220)
        st.divider()

    st.dataframe(df[["article", "designation", "stock", "garantie", "seuil_commande"]], use_container_width=True, height=420)

    st.divider()

    # Seuil global (fallback)
    st.subheader("📦 Pièces à commander")

    global_seuil = int(get_setting("seuil_global", str(DEFAULT_GLOBAL_SEUIL)))
    c1, c2 = st.columns([1, 3])
    with c1:
        new_global = st.number_input("Seuil global", min_value=0, max_value=10000, value=int(global_seuil), step=1)
        if int(new_global) != int(global_seuil):
            set_setting("seuil_global", str(int(new_global)))
            clear_cache()
            st.success("Seuil global mis à jour.")

    # Calcul "à commander" : seuil_piece si présent sinon seuil global
    df2 = df.copy()
    df2["seuil_effectif"] = df2["seuil_commande"].fillna(int(new_global)).astype(int)
    to_order = df2[df2["stock"].astype(int) <= df2["seuil_effectif"].astype(int)].copy()
    to_order = to_order.sort_values(["stock", "designation", "article"], ascending=[True, True, True])

    if to_order.empty:
        st.success("✅ Rien à commander.")
    else:
        to_order.insert(0, "📌", "📦")
        st.dataframe(
            to_order[["📌", "article", "designation", "stock", "seuil_effectif"]],
            use_container_width=True,
            height=360,
        )

    st.divider()

    # Garantie (affichage uniquement)
    st.subheader("🧾 Garantie (garantie > 0)")
    en_garantie = df[df["garantie"].astype(int) > 0].copy().sort_values(["garantie", "designation", "article"], ascending=[False, True, True])
    if en_garantie.empty:
        st.info("Aucune pièce en garantie.")
    else:
        en_garantie.insert(0, "📌", "🛡️")
        st.dataframe(en_garantie[["📌", "article", "designation", "garantie"]], use_container_width=True, height=320)

    st.divider()

    # Historique (commentaire rouge)
    st.subheader("Historique")

    hist = load_historique(limit=400).copy()
    if hist.empty:
        st.info("Aucun mouvement pour l’instant.")
    else:
        # icône + rouge sur commentaire
        hist["commentaire"] = hist["commentaire"].apply(with_comment_icon)

        sty = hist.style.applymap(style_comment_red, subset=["commentaire"])
        st.dataframe(sty, use_container_width=True, height=420)

    st.divider()

    # Supprimer un article tout en bas
    st.subheader("🗑️ Supprimer un article (tout en bas)")
    st.caption("Supprime l'article + son historique. Action irréversible.")

    all_articles = df["article"].astype(str).tolist()
    art_del = st.selectbox("Article à supprimer", [""] + all_articles, index=0)
    confirm = st.checkbox("✅ Je confirme la suppression définitive")

    if st.button("🗑️ Supprimer définitivement", disabled=(not art_del or not confirm), use_container_width=True):
        try:
            delete_article(str(art_del))
            clear_cache()
            st.success("✅ Article supprimé.")
        except Exception as e:
            st.error(f"Erreur suppression : {e}")

# =========================================
# TAB 3 : ADRESSES
# =========================================
with tab_addr:
    st.subheader("📍 Adresses")
    st.caption("Ces adresses servent pour les sorties/entrées avec destination.")

    adresses = load_adresses()
    if not adresses:
        st.info("Aucune adresse enregistrée.")
    else:
        st.dataframe(pd.DataFrame({"Adresse": adresses}), use_container_width=True, height=260)

    st.divider()

    new_a = st.text_input("Ajouter une adresse", placeholder="Ex: Chantier A, Client X, Atelier...").strip()
    if st.button("➕ Ajouter", disabled=(not new_a)):
        exec_sql("INSERT INTO adresses(nom) VALUES (:n) ON CONFLICT (nom) DO NOTHING", {"n": new_a})
        clear_cache()
        st.success("Adresse ajoutée.")

    st.divider()

    if adresses:
        a_del = st.selectbox("Supprimer une adresse", [""] + adresses)
        if st.button("🗑️ Supprimer l'adresse", disabled=(not a_del)):
            exec_sql("DELETE FROM adresses WHERE nom = :n", {"n": a_del})
            clear_cache()
            st.success("Adresse supprimée.")
