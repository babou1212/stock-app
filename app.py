from __future__ import annotations

import pandas as pd
import streamlit as st
from datetime import date

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# =========================================================
# CONFIG
# =========================================================
st.set_page_config(page_title="Gestion de stock", layout="wide")

APP_TITLE = "Gestion de stock"
DEFAULT_GLOBAL_SEUIL = 3  # si une pièce n'a pas son seuil propre


# =========================================================
# DB (Supabase Postgres via SQLAlchemy)
# =========================================================
def get_engine() -> Engine:
    db_url = (st.secrets.get("DB_URL", "") or "").strip()
    if not db_url:
        st.error("DB_URL manquant. Ajoute DB_URL dans Streamlit > Settings > Secrets.")
        st.stop()
    return create_engine(db_url, pool_pre_ping=True)


ENGINE = get_engine()


def exec_sql(sql: str, params: dict | None = None) -> None:
    with ENGINE.begin() as conn:
        conn.execute(text(sql), params or {})


def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    with ENGINE.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def init_db() -> None:
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS articles (
            article TEXT PRIMARY KEY,
            designation TEXT NOT NULL,
            stock INTEGER NOT NULL DEFAULT 0,
            garantie INTEGER NOT NULL DEFAULT 0,
            seuil_commande INTEGER NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS mouvements (
            id BIGSERIAL PRIMARY KEY,
            date DATE NOT NULL,
            article TEXT NOT NULL,
            designation TEXT NOT NULL,
            type_mvt TEXT NOT NULL,
            emplacement TEXT NOT NULL,
            quantite INTEGER NOT NULL,
            adresse TEXT NULL,
            commentaire TEXT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS adresses (
            id BIGSERIAL PRIMARY KEY,
            nom TEXT NOT NULL UNIQUE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """,
        """
        ALTER TABLE articles
        ADD COLUMN IF NOT EXISTS seuil_commande INTEGER NULL;
        """,
    ]
    for q in ddl:
        exec_sql(q)

    existing = read_df("SELECT value FROM settings WHERE key='seuil_global' LIMIT 1;")
    if existing.empty:
        exec_sql(
            "INSERT INTO settings(key, value) VALUES ('seuil_global', :v) ON CONFLICT (key) DO NOTHING;",
            {"v": str(DEFAULT_GLOBAL_SEUIL)},
        )


init_db()


# =========================================================
# SETTINGS
# =========================================================
def get_setting(key: str, default: str) -> str:
    df = read_df("SELECT value FROM settings WHERE key=:k LIMIT 1;", {"k": key})
    return default if df.empty else str(df.iloc[0]["value"])


def set_setting(key: str, value: str) -> None:
    exec_sql(
        """
        INSERT INTO settings(key, value)
        VALUES (:k, :v)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
        """,
        {"k": key, "v": value},
    )


# =========================================================
# ARTICLES / ADRESSES / HISTORIQUE
# =========================================================
def get_articles() -> pd.DataFrame:
    return read_df(
        "SELECT article, designation, stock, garantie, seuil_commande FROM articles ORDER BY article;"
    )


def get_article_row(article: str) -> pd.DataFrame:
    return read_df(
        "SELECT article, designation, stock, garantie, seuil_commande FROM articles WHERE article=:a LIMIT 1;",
        {"a": article},
    )


def add_or_update_article(article: str, designation: str, seuil_commande: int | None) -> None:
    """
    - crée l'article si absent
    - met à jour designation et seuil_commande si fourni
    """
    exec_sql(
        """
        INSERT INTO articles(article, designation, stock, garantie, seuil_commande)
        VALUES (:a, :d, 0, 0, :s)
        ON CONFLICT (article) DO UPDATE
        SET designation = EXCLUDED.designation,
            seuil_commande = COALESCE(EXCLUDED.seuil_commande, articles.seuil_commande);
        """,
        {"a": str(article), "d": designation, "s": seuil_commande},
    )


def update_article_seuil(article: str, seuil: int | None) -> None:
    exec_sql(
        "UPDATE articles SET seuil_commande=:s WHERE article=:a;",
        {"s": seuil, "a": article},
    )


def add_adresse_if_needed(nom: str) -> None:
    nom = (nom or "").strip()
    if not nom:
        return
    exec_sql("INSERT INTO adresses(nom) VALUES (:n) ON CONFLICT (nom) DO NOTHING;", {"n": nom})


def get_adresses() -> list[str]:
    df = read_df("SELECT nom FROM adresses ORDER BY nom;")
    return [] if df.empty else df["nom"].astype(str).tolist()


def get_hist() -> pd.DataFrame:
    return read_df(
        """
        SELECT id, date, article, designation, type_mvt, emplacement, quantite, adresse, commentaire
        FROM mouvements
        ORDER BY id DESC;
        """
    )


def apply_movement(d: date, article: str, designation: str, type_mvt: str, emplacement: str,
                   qty: int, adresse: str | None, commentaire: str | None) -> None:
    df = read_df("SELECT stock, garantie FROM articles WHERE article=:a LIMIT 1;", {"a": article})
    if df.empty:
        raise ValueError("Article inconnu. Ajoute une désignation (1ère fois).")

    stock = int(df.iloc[0]["stock"])
    garantie = int(df.iloc[0]["garantie"])
    if qty <= 0:
        raise ValueError("Quantité invalide.")

    new_stock, new_garantie = stock, garantie

    if emplacement == "STOCK":
        if type_mvt == "ENTREE":
            new_stock += qty
        else:
            new_stock -= qty
    else:  # GARANTIE
        if type_mvt == "ENTREE":
            new_garantie += qty
            new_stock -= qty  # déduction auto du stock
        else:
            new_garantie -= qty

    if new_stock < 0:
        raise ValueError("Stock insuffisant pour cette opération.")
    if new_garantie < 0:
        raise ValueError("Garantie insuffisante pour cette opération.")

    exec_sql(
        "UPDATE articles SET stock=:s, garantie=:g WHERE article=:a;",
        {"s": new_stock, "g": new_garantie, "a": article},
    )

    exec_sql(
        """
        INSERT INTO mouvements(date, article, designation, type_mvt, emplacement, quantite, adresse, commentaire)
        VALUES (:dt, :a, :d, :t, :e, :q, :adr, :c);
        """,
        {
            "dt": d,
            "a": article,
            "d": designation,
            "t": type_mvt,
            "e": emplacement,
            "q": int(qty),
            "adr": (adresse or None),
            "c": (commentaire or None),
        },
    )


def delete_article(article: str) -> None:
    exec_sql("DELETE FROM mouvements WHERE article=:a;", {"a": article})
    exec_sql("DELETE FROM articles WHERE article=:a;", {"a": article})


# =========================================================
# STYLE (remarque rouge + icône)
# =========================================================
def historise_display(df_hist: pd.DataFrame) -> pd.DataFrame:
    df = df_hist.copy()
    df["remarque"] = df["commentaire"].fillna("").astype(str)
    df["remarque"] = df["remarque"].apply(lambda x: f"⚠️ {x}" if x.strip() else "")
    df = df.drop(columns=["commentaire"], errors="ignore")
    df = df.rename(columns={"type_mvt": "type", "quantite": "qté"})
    return df


def style_remarque_red(df_display: pd.DataFrame):
    def color_remarque(val: str):
        if isinstance(val, str) and val.strip():
            return "color: red; font-weight: 700;"
        return ""
    if "remarque" not in df_display.columns:
        return None
    return df_display.style.applymap(color_remarque, subset=["remarque"])


# =========================================================
# UI
# =========================================================
st.title(APP_TITLE)

tab1, tab2, tab3 = st.tabs(["➕ Mouvement", "📦 Stock / Garantie / Gestion", "📍 Adresses"])

# ---------------------------------------------------------
# TAB 1 — MOUVEMENT
# ---------------------------------------------------------
with tab1:
    st.subheader("Ajouter un mouvement")

    colL, colM, colR = st.columns([1.2, 1.2, 1.2])

    with colL:
        d = st.date_input("Date", value=date.today())
        article_input = st.text_input("Numéro d'article", placeholder="Ex: 155082").strip()

    # récupère l'article si existe
    row = get_article_row(article_input) if article_input else pd.DataFrame()
    exists = (not row.empty)

    with colM:
        emplacement = st.selectbox("Emplacement", ["STOCK", "GARANTIE"])
        type_mvt = st.selectbox("Type", ["ENTREE", "SORTIE"])
        qty = st.number_input("Quantité", min_value=1, max_value=10_000, value=1, step=1)

    # designation
    with colL:
        if exists:
            designation = st.text_input("Désignation (auto)", value=str(row.iloc[0]["designation"]), disabled=True)
        else:
            designation = st.text_input("Désignation (1ère fois)", placeholder="Ex: Sonde O2")

    # ✅ SEUIL PAR PIÈCE AU MOMENT DE LA SAISIE
    with colM:
        if exists:
            current_seuil = row.iloc[0]["seuil_commande"]
            current_seuil = None if pd.isna(current_seuil) else int(current_seuil)
            st.caption("Seuil (limite) actuel de cette pièce : " + (str(current_seuil) if current_seuil is not None else "aucun (seuil global)"))
            seuil_piece = st.number_input(
                "Seuil de commande pour cet article (optionnel)",
                min_value=0,
                max_value=10_000,
                value=(current_seuil if current_seuil is not None else 0),
                step=1,
                help="Si Stock ≤ ce seuil => apparaît dans Pièces à commander. Mets 0 si tu veux le forcer à apparaître quand stock=0. "
                     "Si tu veux revenir au seuil global, coche la case juste dessous.",
            )
            use_global = st.checkbox("Utiliser le seuil global (ignorer seuil pièce)", value=(current_seuil is None))
            seuil_to_save = None if use_global else int(seuil_piece)
        else:
            st.caption("Tu peux définir le seuil (limite) de la pièce dès maintenant.")
            seuil_piece_new = st.number_input(
                "Seuil de commande pour cet article (optionnel)",
                min_value=0,
                max_value=10_000,
                value=0,
                step=1,
                help="Laisse 0 si tu ne veux pas mettre de seuil maintenant, ou mets un chiffre.",
            )
            set_now = st.checkbox("Enregistrer ce seuil pour cette pièce", value=False)
            seuil_to_save = int(seuil_piece_new) if set_now else None

    # Adresse destination seulement si SORTIE depuis STOCK
    adresse = None
    if emplacement == "STOCK" and type_mvt == "SORTIE":
        with colR:
            st.markdown("**Adresse destination (optionnel)**")
            adresses = get_adresses()
            options = ["(Aucune)"] + adresses + ["➕ Nouvelle adresse"]
            choix = st.selectbox("Adresse destination", options)

            if choix == "(Aucune)":
                adresse = None
            elif choix == "➕ Nouvelle adresse":
                new_adr = st.text_input("Nouvelle adresse destination", placeholder="Ex: Chantier A, Client X...")
                adresse = new_adr.strip() if new_adr.strip() else None
            else:
                adresse = choix

    with colR:
        commentaire = st.text_area("Remarque / commentaire (optionnel)", height=90)

        if st.button("✅ Enregistrer", use_container_width=True):
            try:
                if not article_input:
                    raise ValueError("Numéro d'article obligatoire.")
                if not designation.strip() and not exists:
                    raise ValueError("Désignation obligatoire (au moins la première fois).")

                final_designation = str(row.iloc[0]["designation"]) if exists else designation.strip()

                # crée/maj article + enregistre seuil si choisi
                add_or_update_article(article_input, final_designation, seuil_to_save)

                # adresse
                add_adresse_if_needed(adresse or "")

                # mouvement
                apply_movement(
                    d=d,
                    article=article_input,
                    designation=final_designation,
                    type_mvt=type_mvt,
                    emplacement=emplacement,
                    qty=int(qty),
                    adresse=adresse,
                    commentaire=(commentaire.strip() if commentaire.strip() else None),
                )

                st.success("Mouvement enregistré ✅")
                st.rerun()

            except Exception as e:
                st.error(str(e))


# ---------------------------------------------------------
# TAB 2 — STOCK / GARANTIE / GESTION
# ---------------------------------------------------------
with tab2:
    articles_df = get_articles()

    st.subheader("Stock actuel")
    search_article = st.text_input("🔎 Rechercher un numéro d’article", placeholder="Tape un numéro (ex: 155082)")

    if articles_df.empty:
        st.info("Aucun article enregistré pour l’instant.")
    else:
        df_view = articles_df.copy()
        df_view["article"] = df_view["article"].astype(str)

        if search_article.strip():
            df_view = df_view[df_view["article"].str.contains(search_article.strip(), case=False, na=False)].copy()
            if df_view.empty:
                st.warning("Aucun article trouvé.")
        st.dataframe(
            df_view[["article", "designation", "stock", "garantie", "seuil_commande"]].rename(columns={"seuil_commande": "Seuil pièce"}),
            use_container_width=True,
            height=520
        )

    st.divider()

    colA, colB = st.columns(2)

    with colA:
        st.subheader("📦 Pièces à commander")

        seuil_global = int(get_setting("seuil_global", str(DEFAULT_GLOBAL_SEUIL)))
        new_global = st.number_input(
            "Seuil global (utilisé si 'Seuil pièce' est vide)",
            min_value=0,
            max_value=10_000,
            value=int(seuil_global),
            step=1,
        )
        if int(new_global) != int(seuil_global):
            set_setting("seuil_global", str(int(new_global)))
            st.success("Seuil global mis à jour ✅")
            st.rerun()

        if not articles_df.empty:
            df = get_articles().copy()
            df["seuil_effectif"] = df["seuil_commande"].fillna(int(new_global)).astype(int)
            a_commander = df[df["stock"] <= df["seuil_effectif"]].copy().sort_values(
                ["stock", "designation", "article"]
            )

            if a_commander.empty:
                st.success("Rien à commander ✅")
            else:
                st.dataframe(
                    a_commander[["article", "designation", "stock", "seuil_effectif"]].rename(columns={"seuil_effectif": "Seuil utilisé"}),
                    use_container_width=True,
                    height=360,
                )

    with colB:
        st.subheader("🧾 Garantie (garantie > 0)")

        if articles_df.empty:
            st.info("Aucune pièce en garantie.")
        else:
            df = get_articles().copy()
            en_garantie = df[df["garantie"] > 0].copy().sort_values(
                ["garantie", "designation", "article"],
                ascending=[False, True, True]
            )

            if en_garantie.empty:
                st.info("Aucune pièce en garantie.")
            else:
                st.dataframe(
                    en_garantie[["article", "designation", "garantie"]],
                    use_container_width=True,
                    height=360,
                )

    st.divider()

    st.subheader("Historique")
    hist = get_hist()
    if hist.empty:
        st.info("Aucun mouvement pour l’instant.")
    else:
        display_hist = historise_display(hist)
        styler = style_remarque_red(display_hist)
        if styler is None:
            st.dataframe(display_hist, use_container_width=True, height=520)
        else:
            st.dataframe(styler, use_container_width=True, height=520)

    st.divider()

    st.subheader("🗑️ Supprimer un article")
    if articles_df.empty:
        st.info("Aucun article à supprimer.")
    else:
        art_to_del = st.selectbox("Article à supprimer", get_articles()["article"].astype(str).tolist())
        st.warning("Supprime l'article + son historique + ses sorties adressées. Action irréversible.")
        if st.button("🗑️ Supprimer définitivement", use_container_width=True):
            try:
                delete_article(art_to_del)
                st.success("Article supprimé ✅")
                st.rerun()
            except Exception as e:
                st.error(str(e))


# ---------------------------------------------------------
# TAB 3 — ADRESSES
# ---------------------------------------------------------
with tab3:
    st.subheader("📍 Sorties par adresse (destination)")

    adresses = get_adresses()
    if not adresses:
        st.info("Aucune adresse enregistrée pour l’instant.")
    else:
        adr = st.selectbox("Choisir une adresse", adresses)

        df_adr = read_df(
            """
            SELECT date, article, designation, quantite, commentaire
            FROM mouvements
            WHERE emplacement='STOCK'
              AND type_mvt='SORTIE'
              AND adresse = :adr
            ORDER BY date DESC, id DESC;
            """,
            {"adr": adr},
        )

        if df_adr.empty:
            st.info("Aucune sortie assignée à cette adresse.")
        else:
            df_adr["remarque"] = df_adr["commentaire"].fillna("").astype(str)
            df_adr["remarque"] = df_adr["remarque"].apply(lambda x: f"⚠️ {x}" if x.strip() else "")
            df_adr = df_adr.drop(columns=["commentaire"], errors="ignore")

            styler = df_adr.style.applymap(
                lambda v: "color:red;font-weight:700;" if isinstance(v, str) and v.strip() else "",
                subset=["remarque"],
            )
            st.dataframe(styler, use_container_width=True, height=520)

    st.divider()
    st.subheader("➕ Ajouter une adresse")
    new_adr = st.text_input("Nouvelle adresse", placeholder="Ex: Chantier A, Client X, Atelier...")
    if st.button("Ajouter l’adresse"):
        try:
            add_adresse_if_needed(new_adr)
            st.success("Adresse ajoutée ✅")
            st.rerun()
        except Exception as e:
            st.error(str(e))
