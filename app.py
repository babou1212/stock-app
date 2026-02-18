from __future__ import annotations

from datetime import date
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

st.set_page_config(page_title="Gestion de stock", layout="wide")

SEUIL_COMMANDE = 3


# -------------------------
# DB (Postgres via SQLAlchemy)
# -------------------------
def get_engine() -> Engine:
    """
    DB_URL doit être dans .streamlit/secrets.toml (en local)
    ou dans Settings > Secrets (sur Streamlit Cloud)
    Exemple:
    DB_URL = "postgresql+psycopg2://user:pass@host:5432/dbname"
    """
    db_url = st.secrets.get("DB_URL", "").strip()
    if not db_url:
        st.error("DB_URL manquant. Ajoute DB_URL dans les Secrets Streamlit.")
        st.stop()

    # pool_pre_ping évite les connexions mortes
    return create_engine(db_url, pool_pre_ping=True)


ENGINE = get_engine()


def init_db():
    # Création tables (Postgres)
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS articles (
            article TEXT PRIMARY KEY,
            designation TEXT NOT NULL,
            stock INTEGER NOT NULL DEFAULT 0,
            garantie INTEGER NOT NULL DEFAULT 0
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS mouvements (
            id BIGSERIAL PRIMARY KEY,
            date DATE NOT NULL,
            article TEXT NOT NULL,
            designation TEXT NOT NULL,
            type TEXT NOT NULL,          -- ENTREE / SORTIE / TRANSFERT
            emplacement TEXT NOT NULL,   -- STOCK / GARANTIE / STOCK->GARANTIE
            quantite INTEGER NOT NULL,
            commentaire TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS adresses (
            id BIGSERIAL PRIMARY KEY,
            adresse TEXT NOT NULL UNIQUE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS sorties (
            id BIGSERIAL PRIMARY KEY,
            date DATE NOT NULL,
            article TEXT NOT NULL,
            designation TEXT NOT NULL,
            emplacement TEXT NOT NULL,   -- STOCK / GARANTIE
            quantite INTEGER NOT NULL,
            adresse TEXT NOT NULL,
            commentaire TEXT
        );
        """,
    ]

    with ENGINE.begin() as conn:
        for q in ddl:
            conn.execute(text(q))


def read_df(query: str, params: dict | None = None) -> pd.DataFrame:
    with ENGINE.connect() as conn:
        return pd.read_sql(text(query), conn, params=params or {})


def exec_sql(query: str, params: dict | None = None):
    with ENGINE.begin() as conn:
        conn.execute(text(query), params or {})


# -------------------------
# Data access
# -------------------------
def get_articles() -> pd.DataFrame:
    return read_df(
        """
        SELECT article, designation, stock, garantie
        FROM articles
        ORDER BY designation, article
        """
    )


def get_designation(article: str) -> str:
    df = read_df("SELECT designation FROM articles WHERE article = :a", {"a": article})
    return df.iloc[0]["designation"] if not df.empty else ""


def add_or_update_article(article: str, designation: str):
    exec_sql(
        """
        INSERT INTO articles(article, designation)
        VALUES (:a, :d)
        ON CONFLICT (article) DO UPDATE SET designation = EXCLUDED.designation
        """,
        {"a": article, "d": designation},
    )


def add_mouvement(d: date, article: str, designation: str, type_mvt: str, emplacement: str, qty: int, commentaire: str):
    exec_sql(
        """
        INSERT INTO mouvements(date, article, designation, type, emplacement, quantite, commentaire)
        VALUES (:dt, :a, :d, :t, :e, :q, :c)
        """,
        {"dt": d, "a": article, "d": designation, "t": type_mvt, "e": emplacement, "q": int(qty), "c": commentaire},
    )


def get_historique() -> pd.DataFrame:
    return read_df("SELECT * FROM mouvements ORDER BY id DESC")


def ensure_adresse(adresse: str):
    adresse = (adresse or "").strip()
    if not adresse:
        return
    exec_sql("INSERT INTO adresses(adresse) VALUES (:ad) ON CONFLICT (adresse) DO NOTHING", {"ad": adresse})


def get_adresses() -> list[str]:
    df = read_df("SELECT adresse FROM adresses ORDER BY adresse")
    return df["adresse"].tolist() if not df.empty else []


def add_sortie(d: date, article: str, designation: str, emplacement: str, qty: int, adresse: str, commentaire: str):
    exec_sql(
        """
        INSERT INTO sorties(date, article, designation, emplacement, quantite, adresse, commentaire)
        VALUES (:dt, :a, :d, :e, :q, :ad, :c)
        """,
        {"dt": d, "a": article, "d": designation, "e": emplacement, "q": int(qty), "ad": adresse, "c": commentaire},
    )


def get_sorties() -> pd.DataFrame:
    return read_df("SELECT * FROM sorties ORDER BY id DESC")


def delete_article(article: str):
    exec_sql("DELETE FROM sorties WHERE article = :a", {"a": article})
    exec_sql("DELETE FROM mouvements WHERE article = :a", {"a": article})
    exec_sql("DELETE FROM articles WHERE article = :a", {"a": article})


def rename_article(old_article: str, new_article: str, new_designation: str):
    # collision ?
    df = read_df("SELECT article FROM articles WHERE article = :a", {"a": new_article})
    if not df.empty and new_article != old_article:
        return False, "Ce nouveau numéro d’article existe déjà ❌"

    df2 = read_df("SELECT stock, garantie FROM articles WHERE article = :a", {"a": old_article})
    if df2.empty:
        return False, "Article introuvable ❌"

    stock = int(df2.iloc[0]["stock"])
    garantie = int(df2.iloc[0]["garantie"])

    if new_article != old_article:
        # recrée la clé + update historiques
        exec_sql("DELETE FROM articles WHERE article = :a", {"a": old_article})
        exec_sql(
            """
            INSERT INTO articles(article, designation, stock, garantie)
            VALUES (:a, :d, :s, :g)
            """,
            {"a": new_article, "d": new_designation, "s": stock, "g": garantie},
        )
        exec_sql(
            "UPDATE mouvements SET article = :na, designation = :nd WHERE article = :oa",
            {"na": new_article, "nd": new_designation, "oa": old_article},
        )
        exec_sql(
            "UPDATE sorties SET article = :na, designation = :nd WHERE article = :oa",
            {"na": new_article, "nd": new_designation, "oa": old_article},
        )
    else:
        exec_sql("UPDATE articles SET designation = :d WHERE article = :a", {"d": new_designation, "a": old_article})
        exec_sql("UPDATE mouvements SET designation = :d WHERE article = :a", {"d": new_designation, "a": old_article})
        exec_sql("UPDATE sorties SET designation = :d WHERE article = :a", {"d": new_designation, "a": old_article})

    return True, "Article modifié ✅"


# -------------------------
# Business logic
# -------------------------
def update_emplacement(article: str, qty: int, mouvement: str, emplacement: str):
    """
    - ENTREE + STOCK     => stock += qty
    - SORTIE + STOCK     => stock -= qty (pas négatif)
    - ENTREE + GARANTIE  => transfert auto Stock->Garantie (stock -= qty, garantie += qty)
    - SORTIE + GARANTIE  => garantie -= qty (pas négatif)
    """
    emplacement = emplacement.upper()
    if emplacement not in ("STOCK", "GARANTIE"):
        return False, "Emplacement invalide ❌"

    # verrouille la ligne article (évite conflits multi-appareils)
    with ENGINE.begin() as conn:
        row = conn.execute(
            text("SELECT stock, garantie, designation FROM articles WHERE article = :a FOR UPDATE"),
            {"a": article},
        ).fetchone()

        if not row:
            return False, "Article introuvable ❌"

        stock, garantie, designation = int(row.stock), int(row.garantie), row.designation
        qty = int(qty)

        if mouvement == "ENTREE":
            if emplacement == "STOCK":
                conn.execute(text("UPDATE articles SET stock = stock + :q WHERE article = :a"), {"q": qty, "a": article})
                type_mvt, emp = "ENTREE", "STOCK"
            else:
                # ENTREE en GARANTIE = transfert stock -> garantie
                if qty > stock:
                    return False, f"Stock insuffisant pour mettre en garantie ❌ (dispo: {stock})"
                conn.execute(
                    text("UPDATE articles SET stock = stock - :q, garantie = garantie + :q WHERE article = :a"),
                    {"q": qty, "a": article},
                )
                type_mvt, emp = "TRANSFERT", "STOCK->GARANTIE"

        elif mouvement == "SORTIE":
            if emplacement == "STOCK":
                if qty > stock:
                    return False, f"Stock insuffisant ❌ (dispo: {stock})"
                conn.execute(text("UPDATE articles SET stock = stock - :q WHERE article = :a"), {"q": qty, "a": article})
                type_mvt, emp = "SORTIE", "STOCK"
            else:
                if qty > garantie:
                    return False, f"Garantie insuffisante ❌ (dispo: {garantie})"
                conn.execute(text("UPDATE articles SET garantie = garantie - :q WHERE article = :a"), {"q": qty, "a": article})
                type_mvt, emp = "SORTIE", "GARANTIE"
        else:
            return False, "Mouvement invalide ❌"

    return True, (designation, type_mvt, emp)


# -------------------------
# UI helpers
# -------------------------
def adresse_destination_picker(prefix: str) -> str:
    # sans champ "Rechercher..."
    all_addr = get_adresses()
    options = list(all_addr)
    options.insert(0, "➕ Nouvelle adresse")

    chosen = st.selectbox("Adresse destination", options=options, key=f"{prefix}_select")
    if chosen == "➕ Nouvelle adresse":
        new_addr = st.text_input("Nouvelle adresse destination", key=f"{prefix}_new")
        return (new_addr or "").strip()
    return (chosen or "").strip()


def add_warning_and_style(df: pd.DataFrame):
    """Ajoute colonne ⚠ si commentaire non vide + commentaire en rouge/gras."""
    if df.empty:
        return None

    df2 = df.copy()

    if "commentaire" in df2.columns:
        df2.insert(
            0,
            "⚠",
            df2["commentaire"].apply(lambda x: "⚠️" if isinstance(x, str) and x.strip() else ""),
        )
    else:
        df2.insert(0, "⚠", "")

    def highlight_comment(val):
        if isinstance(val, str) and val.strip():
            return "color: red; font-weight: bold;"
        return ""

    if "commentaire" in df2.columns:
        return df2.style.applymap(highlight_comment, subset=["commentaire"])
    return df2.style


# -------------------------
# APP
# -------------------------
init_db()
st.title("📦 Gestion de stock")

tab1, tab2, tab3 = st.tabs(["➕ Mouvement", "📊 Stock / Garantie / Gestion", "📍 Adresses"])


# ========= TAB 1 =========
with tab1:
    st.subheader("Ajouter un mouvement")
    c1, c2, c3 = st.columns(3)

    with c1:
        d = st.date_input("Date", value=date.today())
        article = st.text_input("Numéro d’article", placeholder="Ex: 155082").strip()
        designation_db = get_designation(article) if article else ""

    with c2:
        emplacement = st.selectbox("Emplacement", ["STOCK", "GARANTIE"])
        mouvement = st.selectbox("Type", ["ENTREE", "SORTIE"])
        qty = st.number_input("Quantité", min_value=1, step=1, value=1)

        if article and designation_db:
            st.text_input("Désignation (auto)", value=designation_db, disabled=True)
            designation = designation_db
        else:
            designation = st.text_input("Désignation (1ère fois)", placeholder="Ex: Sonde O2").strip()

    with c3:
        adresse_dest = ""
        if mouvement == "SORTIE":
            st.markdown("**Adresse destination (optionnel)**")
            adresse_dest = adresse_destination_picker("sortie_addr")
            st.caption("Si vide, la sortie ne sera pas assignée à une adresse.")
        commentaire = st.text_area("Remarque / commentaire (optionnel)")
        st.write("")
        if st.button("✅ Enregistrer", use_container_width=True):
            if not article:
                st.error("Merci de renseigner le numéro d’article.")
            elif not designation:
                st.error("Merci de renseigner la désignation (au moins la première fois).")
            else:
                add_or_update_article(article, designation)

                ok, info = update_emplacement(article, int(qty), mouvement, emplacement)
                if ok:
                    designation_ok, type_mvt, emp = info
                    add_mouvement(d, article, designation_ok, type_mvt, emp, int(qty), commentaire.strip())

                    if mouvement == "SORTIE" and adresse_dest.strip():
                        ensure_adresse(adresse_dest)
                        add_sortie(d, article, designation_ok, emplacement, int(qty), adresse_dest.strip(), commentaire.strip())

                    st.success("Mouvement enregistré ✅")
                    st.rerun()
                else:
                    st.error(info)

    st.divider()
    st.subheader("✏️ Modifier numéro / désignation (en bas de Mouvement)")
    df_mod = get_articles()
    if df_mod.empty:
        st.info("Aucun article à modifier.")
    else:
        art_mod = st.selectbox("Choisir l’article", df_mod["article"].tolist(), key="mod_art")
        current_des = df_mod[df_mod["article"] == art_mod].iloc[0]["designation"]

        new_article = st.text_input("Nouveau numéro", value=art_mod, key="mod_new_article").strip()
        new_designation = st.text_input("Nouvelle désignation", value=current_des, key="mod_new_des").strip()

        if st.button("💾 Enregistrer modifications", use_container_width=True, key="btn_mod"):
            if not new_article or not new_designation:
                st.error("Numéro et désignation ne peuvent pas être vides.")
            else:
                ok, msg = rename_article(art_mod, new_article, new_designation)
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)


# ========= TAB 2 =========
with tab2:
    df = get_articles()
    if df.empty:
        st.info("Aucun article enregistré pour l’instant.")
    else:
        st.subheader("Stock actuel")

        search_article = st.text_input("🔎 Rechercher par numéro d’article", placeholder="Ex: 155082").strip()

        if search_article:
            df_view = df[df["article"].astype(str).str.contains(search_article, case=False, na=False)].copy()
            if df_view.empty:
                st.warning("Aucun article trouvé.")
            else:
                st.dataframe(df_view[["article", "designation", "stock"]], use_container_width=True, height=260)
        else:
            st.dataframe(df[["article", "designation", "stock"]], use_container_width=True, height=520)

        st.divider()
        colA, colB = st.columns(2)

        with colA:
            st.subheader(f"🛒 Pièces à commander (stock ≤ {SEUIL_COMMANDE})")
            a_commander = df[df["stock"] <= SEUIL_COMMANDE].copy().sort_values(["stock", "designation", "article"])
            if a_commander.empty:
                st.success("Rien à commander ✅")
            else:
                st.dataframe(a_commander[["article", "designation", "stock"]],
                             use_container_width=True, height=360)

        with colB:
            st.subheader("🧾 Garantie (garantie > 0)")
            en_garantie = df[df["garantie"] > 0].copy().sort_values(
                ["garantie", "designation", "article"], ascending=[False, True, True]
            )
            if en_garantie.empty:
                st.info("Aucune pièce en garantie.")
            else:
                st.dataframe(en_garantie[["article", "designation", "garantie"]],
                             use_container_width=True, height=360)

    st.divider()
    st.subheader("Historique")
    hist = get_historique().drop(columns=["id"], errors="ignore")
    styled_hist = add_warning_and_style(hist)
    if styled_hist is None:
        st.info("Aucun mouvement pour l’instant.")
    else:
        st.dataframe(styled_hist, use_container_width=True, height=420)

    st.divider()
    st.subheader("🗑 Supprimer un article")
    df_del = get_articles()
    if df_del.empty:
        st.info("Aucun article à supprimer.")
    else:
        art_del = st.selectbox("Article à supprimer", df_del["article"].tolist(), key="art_del")
        st.warning("Supprime l’article + son historique + ses sorties adressées. Action irréversible.")
        if st.button("🗑 Supprimer définitivement", use_container_width=True):
            delete_article(art_del)
            st.success("Article supprimé ✅")
            st.rerun()


# ========= TAB 3 =========
with tab3:
    st.subheader("📍 Adresses - pièces utilisées (sorties)")

    sorties = get_sorties()
    if sorties.empty:
        st.info("Aucune sortie assignée à une adresse pour l’instant.")
    else:
        adresses = sorted(sorties["adresse"].dropna().unique().tolist())
        adresse_choisie = st.selectbox("Choisir une adresse", adresses)

        view = sorties[sorties["adresse"] == adresse_choisie].copy()
        view = view.sort_values(["date", "id"], ascending=[False, False])

        cols = ["date", "article", "designation", "emplacement", "quantite", "commentaire"]
        view = view[cols].copy()

        styled_view = add_warning_and_style(view)
        st.dataframe(styled_view, use_container_width=True, height=520)

        st.download_button(
            "⬇️ Exporter cette adresse (CSV)",
            data=view.to_csv(index=False).encode("utf-8"),
            file_name=f"sorties_{adresse_choisie}.csv",
            mime="text/csv",
            use_container_width=True,

        )
