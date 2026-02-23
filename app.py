# app.py
# ------------------------------------------------------------
# Gestion de stock (Streamlit + PostgreSQL/Supabase)
# - Tables: articles, mouvements, adresses, garanties, settings
# - Fix :
#   * settings créé (plus d'erreur SELECT settings)
#   * adresses avec contrainte UNIQUE (ON CONFLICT OK)
#   * ajout adresse : refuse vide -> plus de NotNullViolation
#   * mouvements : date_mvt toujours rempli + INSERT correct
#   * désignation auto si article existe (sans callback dans form)
#   * pas de callback Streamlit dans st.form (plus d'erreur)
# ------------------------------------------------------------

import os
import datetime as dt
from dataclasses import dataclass

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# -----------------------------
# CONFIG / DB
# -----------------------------

APP_TITLE = "Gestion de stock"

def _get_database_url() -> str | None:
    # Streamlit Cloud: st.secrets["DATABASE_URL"]
    # Local: env DATABASE_URL
    url = None
    try:
        url = st.secrets.get("DATABASE_URL")  # type: ignore[attr-defined]
    except Exception:
        url = None
    url = url or os.getenv("DATABASE_URL")

    if not url:
        return None

    # Heroku-like "postgres://" -> SQLAlchemy wants "postgresql://"
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]

    return url


@st.cache_resource(show_spinner=False)
def get_engine() -> Engine:
    url = _get_database_url()
    if not url:
        st.error("❌ Variable d'environnement DATABASE_URL manquante.")
        st.stop()

    # Supabase / pooler: SSL requis dans la plupart des cas
    connect_args = {}
    if "sslmode=" not in url:
        connect_args["sslmode"] = "require"

    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args=connect_args,
        future=True,
    )


def exec_sql(sql: str, params: dict | None = None) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})


def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


# -----------------------------
# INIT DB (création + "patch" contraintes)
# -----------------------------

def init_db() -> None:
    # Tables principales (IF NOT EXISTS)
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS settings (
            k TEXT PRIMARY KEY,
            v INTEGER NOT NULL
        );
        """
    )

    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS articles (
            article TEXT PRIMARY KEY,
            designation TEXT,
            stock INTEGER NOT NULL DEFAULT 0,
            seuil_piece INTEGER NOT NULL DEFAULT 0
        );
        """
    )

    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS garanties (
            article TEXT PRIMARY KEY REFERENCES articles(article) ON DELETE CASCADE,
            garantie INTEGER NOT NULL DEFAULT 0
        );
        """
    )

    # Adresses: on normalise sur colonne "adresse"
    # (si tu avais déjà une table adresses(nom) on s'adapte plus bas)
    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS adresses (
            id SERIAL PRIMARY KEY,
            adresse TEXT NOT NULL
        );
        """
    )

    exec_sql(
        """
        CREATE TABLE IF NOT EXISTS mouvements (
            id SERIAL PRIMARY KEY,
            date_mvt DATE NOT NULL,
            article TEXT NOT NULL REFERENCES articles(article) ON DELETE CASCADE,
            designation TEXT,
            type_mvt TEXT NOT NULL CHECK (type_mvt IN ('ENTREE','SORTIE')),
            emplacement TEXT NOT NULL,
            quantite INTEGER NOT NULL CHECK (quantite > 0),
            adresse TEXT,
            commentaire TEXT
        );
        """
    )

    # --- PATCH contraintes / compatibilité ---
    # 1) settings: valeur défaut seuil_global si absent
    exec_sql(
        """
        INSERT INTO settings(k, v)
        VALUES ('seuil_global', 0)
        ON CONFLICT (k) DO NOTHING;
        """
    )

    # 2) adresses: gérer l'ancien schéma possible (colonne "nom")
    cols = read_df(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'adresses';
        """
    )["column_name"].tolist()

    if "nom" in cols and "adresse" not in cols:
        # Migration simple: renommer nom -> adresse
        exec_sql("""ALTER TABLE adresses RENAME COLUMN nom TO adresse;""")

    # 3) contrainte UNIQUE pour ON CONFLICT(adresse)
    exec_sql(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname='public'
                  AND tablename='adresses'
                  AND indexname='adresses_adresse_unique_idx'
            ) THEN
                CREATE UNIQUE INDEX adresses_adresse_unique_idx ON adresses(adresse);
            END IF;
        END $$;
        """
    )


# -----------------------------
# SETTINGS helpers
# -----------------------------

def get_setting_int(key: str, default: int = 0) -> int:
    df = read_df("SELECT v FROM settings WHERE k = :k", {"k": key})
    if df.empty:
        return default
    try:
        return int(df.iloc[0]["v"])
    except Exception:
        return default


def set_setting_int(key: str, value: int) -> None:
    exec_sql(
        """
        INSERT INTO settings(k, v)
        VALUES (:k, :v)
        ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v;
        """,
        {"k": key, "v": int(value)},
    )


# -----------------------------
# ARTICLES / GARANTIES helpers
# -----------------------------

def get_article(article: str) -> dict | None:
    df = read_df(
        """
        SELECT a.article, a.designation, a.stock, a.seuil_piece,
               COALESCE(g.garantie, 0) AS garantie
        FROM articles a
        LEFT JOIN garanties g ON g.article = a.article
        WHERE a.article = :a
        """,
        {"a": article},
    )
    if df.empty:
        return None
    r = df.iloc[0].to_dict()
    return {
        "article": str(r["article"]),
        "designation": (r.get("designation") or "") if r.get("designation") is not None else "",
        "stock": int(r.get("stock") or 0),
        "seuil_piece": int(r.get("seuil_piece") or 0),
        "garantie": int(r.get("garantie") or 0),
    }


def upsert_article(article: str, designation: str | None, seuil_piece: int | None = None) -> None:
    # Si designation est vide -> on ne remplace pas une designation existante
    exec_sql(
        """
        INSERT INTO articles(article, designation, stock, seuil_piece)
        VALUES (:a, NULLIF(:d,''), 0, COALESCE(:s, 0))
        ON CONFLICT (article) DO UPDATE SET
            designation = COALESCE(NULLIF(EXCLUDED.designation,''), articles.designation),
            seuil_piece = COALESCE(EXCLUDED.seuil_piece, articles.seuil_piece);
        """,
        {"a": article, "d": designation or "", "s": seuil_piece},
    )

    # Garanties: garantir une ligne (même si 0)
    exec_sql(
        """
        INSERT INTO garanties(article, garantie)
        VALUES (:a, 0)
        ON CONFLICT (article) DO NOTHING;
        """,
        {"a": article},
    )


def set_garantie(article: str, garantie: int) -> None:
    # Ensure article exists
    upsert_article(article, designation=None, seuil_piece=None)
    exec_sql(
        """
        INSERT INTO garanties(article, garantie)
        VALUES (:a, :g)
        ON CONFLICT (article) DO UPDATE SET garantie = EXCLUDED.garantie;
        """,
        {"a": article, "g": int(garantie)},
    )


def apply_movement(article: str, type_mvt: str, quantite: int) -> None:
    # Update stock safely
    cur = get_article(article)
    if not cur:
        # devrait pas arriver car on upsert avant
        raise ValueError("Article introuvable")

    stock = int(cur["stock"])
    q = int(quantite)

    if type_mvt == "ENTREE":
        new_stock = stock + q
    else:
        new_stock = stock - q
        if new_stock < 0:
            raise ValueError(f"Stock insuffisant: stock={stock}, sortie={q}")

    exec_sql(
        "UPDATE articles SET stock = :s WHERE article = :a",
        {"s": int(new_stock), "a": article},
    )


def insert_movement(
    date_mvt: dt.date,
    article: str,
    designation: str,
    type_mvt: str,
    emplacement: str,
    quantite: int,
    adresse: str | None,
    commentaire: str | None,
) -> None:
    exec_sql(
        """
        INSERT INTO mouvements(
            date_mvt, article, designation, type_mvt, emplacement, quantite, adresse, commentaire
        )
        VALUES (
            :date_mvt, :article, NULLIF(:designation,''), :type_mvt, :emplacement, :quantite,
            NULLIF(:adresse,''), NULLIF(:commentaire,'')
        );
        """,
        {
            "date_mvt": date_mvt,
            "article": article,
            "designation": designation or "",
            "type_mvt": type_mvt,
            "emplacement": emplacement,
            "quantite": int(quantite),
            "adresse": (adresse or ""),
            "commentaire": (commentaire or ""),
        },
    )


# -----------------------------
# ADRESSES helpers
# -----------------------------

def list_adresses() -> list[str]:
    df = read_df("SELECT adresse FROM adresses ORDER BY adresse")
    return [str(x) for x in df["adresse"].tolist()] if not df.empty else []


def add_adresse(adresse: str) -> None:
    a = (adresse or "").strip()
    if not a:
        raise ValueError("Adresse vide")
    exec_sql(
        """
        INSERT INTO adresses(adresse)
        VALUES (:a)
        ON CONFLICT (adresse) DO NOTHING;
        """,
        {"a": a},
    )


def delete_adresse(adresse: str) -> None:
    exec_sql("DELETE FROM adresses WHERE adresse = :a", {"a": adresse})


# -----------------------------
# UI
# -----------------------------

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

try:
    init_db()
except Exception as e:
    st.error(f"❌ Erreur init DB: {e}")
    st.stop()

tab_mvt, tab_stock, tab_addr = st.tabs(["Mouvements", "Stock actuel", "Adresses"])


# -----------------------------
# TAB 1: MOUVEMENTS
# -----------------------------
with tab_mvt:
    st.subheader("Ajouter un mouvement")

    # IMPORTANT: pas de callback "on_change" dans un st.form
    # On met l'article HORS du form pour pouvoir rafraîchir la désignation immédiatement.
    colA, colB = st.columns([1.2, 2])

    with colA:
        article_input = st.text_input("Numéro d’article", key="mvt_article").strip()
        existing = get_article(article_input) if article_input else None

        auto_design = existing["designation"] if (existing and existing["designation"]) else ""
        st.caption("Désignation (auto si article existant)")
        designation_input = st.text_input(
            "Désignation",
            value=auto_design,
            key="mvt_designation",
            help="Si l’article existe, la désignation est pré-remplie. Sinon tu peux la saisir (ou laisser vide).",
        ).strip()

        if existing and existing["designation"] and designation_input != existing["designation"]:
            st.info("ℹ️ Tu as modifié la désignation. Elle sera enregistrée si tu soumets le mouvement.")

    with colB:
        adrs = [""] + list_adresses()

        with st.form("form_mvt", clear_on_submit=False):
            c1, c2, c3 = st.columns([1.1, 1.1, 1.4])

            with c1:
                date_mvt = st.date_input("Date", value=dt.date.today())
                emplacement = st.selectbox("Emplacement", ["STOCK"], index=0)

            with c2:
                type_mvt = st.selectbox("Type", ["ENTREE", "SORTIE"], index=1)
                quantite = st.number_input("Quantité", min_value=1, max_value=100000, value=1, step=1)

                st.caption("Seuil pièce : 0 = aucun (seuil global utilisé)")
                seuil_piece = st.number_input(
                    "Seuil pièce (optionnel)",
                    min_value=0,
                    max_value=100000,
                    value=0,
                    step=1,
                )
                maj_seuil = st.checkbox(
                    "Mettre à jour le seuil de cette pièce (même si elle existe déjà)",
                    value=True,
                )

            with c3:
                adresse = st.selectbox("Adresse (optionnel)", adrs, index=0)
                commentaire = st.text_area("Remarque / commentaire (optionnel)", height=120)

            submitted = st.form_submit_button("✅ Enregistrer", use_container_width=True)

        if submitted:
            try:
                if not article_input:
                    st.error("❌ Numéro d’article obligatoire.")
                    st.stop()

                # Upsert article + seuil
                if maj_seuil:
                    sp = int(seuil_piece)
                else:
                    sp = None  # ne touche pas au seuil existant

                upsert_article(article_input, designation_input if designation_input else None, sp)

                # Appliquer mouvement + log
                apply_movement(article_input, type_mvt, int(quantite))
                insert_movement(
                    date_mvt=date_mvt,
                    article=article_input,
                    designation=designation_input,
                    type_mvt=type_mvt,
                    emplacement=emplacement,
                    quantite=int(quantite),
                    adresse=adresse,
                    commentaire=commentaire,
                )

                st.success("✅ Mouvement enregistré.")
                st.rerun()

            except Exception as e:
                st.error(f"❌ Erreur enregistrement mouvement: {e}")

    st.divider()
    st.subheader("Historique des mouvements")

    hist = read_df(
        """
        SELECT date_mvt, article, designation, type_mvt, emplacement, quantite, adresse, commentaire
        FROM mouvements
        ORDER BY date_mvt DESC, id DESC
        LIMIT 500
        """
    )
    st.dataframe(hist, use_container_width=True, height=360)


# -----------------------------
# TAB 2: STOCK ACTUEL + PIECES A COMMANDER + GARANTIES
# -----------------------------
with tab_stock:
    st.subheader("Stock actuel")

    search = st.text_input("Recherche", placeholder="Numéro ou mot dans désignation").strip().lower()

    stock_df = read_df(
        """
        SELECT a.article, a.designation, a.stock, a.seuil_piece,
               COALESCE(g.garantie, 0) AS garantie
        FROM articles a
        LEFT JOIN garanties g ON g.article = a.article
        ORDER BY a.article
        """
    )

    view_df = stock_df.copy()
    if search:
        mask = (
            view_df["article"].astype(str).str.lower().str.contains(search, na=False)
            | view_df["designation"].astype(str).str.lower().str.contains(search, na=False)
        )
        view_df = view_df[mask]

    # IMPORTANT: colonne garantie affichée ici (mais tu peux la cacher et gérer via tableau)
    # Si tu veux vraiment l’enlever du tableau principal, décommente la ligne suivante :
    # view_df = view_df.drop(columns=["garantie"], errors="ignore")

    st.dataframe(view_df, use_container_width=True, height=360)

    st.divider()
    st.subheader("Pièces à commander")

    seuil_global = get_setting_int("seuil_global", 0)
    c1, c2 = st.columns([1, 2])
    with c1:
        new_seuil_global = st.number_input("Seuil global (utilisé si seuil pièce = 0)", min_value=0, value=int(seuil_global), step=1)
        if st.button("💾 Enregistrer seuil global", use_container_width=True):
            set_setting_int("seuil_global", int(new_seuil_global))
            st.success("✅ Seuil global mis à jour.")
            st.rerun()

    # Calcul commande: si seuil_piece>0 => seuil_piece sinon seuil_global
    if not stock_df.empty:
        tmp = stock_df.copy()
        tmp["seuil_calc"] = tmp["seuil_piece"].fillna(0).astype(int)
        tmp.loc[tmp["seuil_calc"] == 0, "seuil_calc"] = int(new_seuil_global)
        tmp["a_commander"] = (tmp["seuil_calc"] - tmp["stock"].fillna(0).astype(int)).clip(lower=0)
        to_order = tmp[tmp["a_commander"] > 0][["article", "designation", "stock", "seuil_piece", "seuil_calc", "a_commander"]]
        if to_order.empty:
            st.success("✅ Rien à commander.")
        else:
            st.dataframe(to_order, use_container_width=True, height=260)

    st.divider()
    st.subheader("Tableau garanties (séparé)")

    # Tableau séparé pour gérer garantie proprement
    gar_df = read_df(
        """
        SELECT a.article, a.designation, COALESCE(g.garantie, 0) AS garantie
        FROM articles a
        LEFT JOIN garanties g ON g.article = a.article
        ORDER BY a.article
        """
    )
    st.dataframe(gar_df, use_container_width=True, height=260)

    st.markdown("*Modifier une garantie*")
    if not gar_df.empty:
        col1, col2 = st.columns([1, 1.2])
        with col1:
            art_sel = st.selectbox("Choisir l'article", gar_df["article"].astype(str).tolist())
        row = gar_df[gar_df["article"].astype(str) == str(art_sel)].iloc[0]
        with col2:
            g_val = st.number_input("Garantie (nombre)", min_value=0, max_value=100000, value=int(row["garantie"]), step=1)
            if st.button("💾 Enregistrer la garantie", use_container_width=True):
                try:
                    set_garantie(str(art_sel), int(g_val))
                    st.success("✅ Garantie mise à jour.")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Erreur garantie: {e}")


# -----------------------------
# TAB 3: ADRESSES
# -----------------------------
with tab_addr:
    st.subheader("Ajouter une adresse")

    with st.form("form_add_address", clear_on_submit=True):
        new_addr = st.text_input("Nouvelle adresse", placeholder="Ex: Avenue de Miremont 27A/B, 1206 Genève")
        add_ok = st.form_submit_button("✅ Ajouter", use_container_width=True)

    if add_ok:
        try:
            add_adresse(new_addr)
            st.success("✅ Adresse ajoutée (ou déjà existante).")
            st.rerun()
        except Exception as e:
            st.error(f"❌ Impossible d’ajouter l’adresse: {e}")

    st.divider()
    st.subheader("Supprimer une adresse")

    addr_list = list_adresses()
    if not addr_list:
        st.info("Aucune adresse enregistrée.")
    else:
        addr_del = st.selectbox("Adresse à supprimer", [""] + addr_list, index=0)
        confirm = st.checkbox("Je confirme la suppression")
        if st.button("🗑️ Supprimer l’adresse", disabled=(not addr_del or not confirm), use_container_width=True):
            try:
                delete_adresse(addr_del)
                st.success("✅ Adresse supprimée.")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Erreur suppression: {e}")

    st.divider()
    st.subheader("Liste des adresses")
    st.dataframe(pd.DataFrame({"adresse": addr_list}), use_container_width=True, height=260)
