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
DEFAULT_GLOBAL_SEUIL = 3  # seuil global si une pièce n'a pas de seuil personnalisé


# =========================================================
# DB (Supabase Postgres via SQLAlchemy)
# =========================================================
def get_engine() -> Engine:
    """
    Récupère la DB_URL depuis Streamlit Secrets (Cloud) :
      Settings (de ton app Streamlit) -> Secrets
      DB_URL = "postgresql+psycopg2://...."
    """
    db_url = (st.secrets.get("DB_URL", "") or "").strip()
    if not db_url:
        st.error("DB_URL manquant. Ajoute DB_URL dans Streamlit > Settings > Secrets.")
        st.stop()

    # pool_pre_ping évite les connexions mortes
    return create_engine(db_url, pool_pre_ping=True)


ENGINE = get_engine()


def exec_sql(sql: str, params: dict | None = None) -> None:
    with ENGINE.begin() as conn:
        conn.execute(text(sql), params or {})


def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    with ENGINE.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def init_db() -> None:
    """
    Tables :
      - articles(article PK, designation, stock, garantie, seuil_commande)
      - mouvements(id, date, article, designation, type_mvt, emplacement, quantite, adresse, commentaire)
      - adresses(id, nom UNIQUE)
      - settings(key PK, value)
    """
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
            type_mvt TEXT NOT NULL,         -- "ENTREE" / "SORTIE"
            emplacement TEXT NOT NULL,      -- "STOCK" / "GARANTIE"
            quantite INTEGER NOT NULL,
            adresse TEXT NULL,              -- uniquement si SORTIE STOCK vers adresse
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
        # Sécurité : si tu as une DB ancienne, on tente d'ajouter la colonne seuil_commande
        """
        ALTER TABLE articles
        ADD COLUMN IF NOT EXISTS seuil_commande INTEGER NULL;
        """,
    ]
    for q in ddl:
        exec_sql(q)

    # Met le seuil global par défaut si absent
    existing = read_df("SELECT value FROM settings WHERE key='seuil_global' LIMIT 1;")
    if existing.empty:
        exec_sql(
            "INSERT INTO settings(key, value) VALUES ('seuil_global', :v) ON CONFLICT (key) DO NOTHING;",
            {"v": str(DEFAULT_GLOBAL_SEUIL)},
        )


init_db()


# =========================================================
# HELPERS (settings)
# =========================================================
def get_setting(key: str, default: str) -> str:
    df = read_df("SELECT value FROM settings WHERE key=:k LIMIT 1;", {"k": key})
    if df.empty:
        return default
    return str(df.iloc[0]["value"])


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
# HELPERS (articles / mouvements)
# =========================================================
def get_articles() -> pd.DataFrame:
    return read_df(
        "SELECT article, designation, stock, garantie, seuil_commande FROM articles ORDER BY article;"
    )


def get_designation(article: str) -> str:
    df = read_df("SELECT designation FROM articles WHERE article=:a LIMIT 1;", {"a": article})
    return "" if df.empty else str(df.iloc[0]["designation"])


def add_or_update_article(article: str, designation: str) -> None:
    exec_sql(
        """
        INSERT INTO articles(article, designation, stock, garantie)
        VALUES (:a, :d, 0, 0)
        ON CONFLICT (article) DO UPDATE SET designation = EXCLUDED.designation;
        """,
        {"a": str(article), "d": designation},
    )


def update_article_number_and_designation(old_article: str, new_article: str, new_designation: str) -> None:
    # Si le numéro change, il faut déplacer l'article
    if old_article != new_article:
        # Vérifie si new_article existe déjà
        df_exists = read_df("SELECT 1 FROM articles WHERE article=:a LIMIT 1;", {"a": new_article})
        if not df_exists.empty:
            raise ValueError("Le nouveau numéro existe déjà. Choisis un numéro unique.")

        # Récupère l'ancien article
        df_old = read_df(
            "SELECT article, designation, stock, garantie, seuil_commande FROM articles WHERE article=:a;",
            {"a": old_article},
        )
        if df_old.empty:
            raise ValueError("Article introuvable.")

        stock = int(df_old.iloc[0]["stock"])
        garantie = int(df_old.iloc[0]["garantie"])
        seuil = df_old.iloc[0]["seuil_commande"]
        seuil = None if pd.isna(seuil) else int(seuil)

        # Crée le nouveau + supprime l'ancien
        exec_sql(
            """
            INSERT INTO articles(article, designation, stock, garantie, seuil_commande)
            VALUES (:a, :d, :s, :g, :sc);
            """,
            {"a": new_article, "d": new_designation, "s": stock, "g": garantie, "sc": seuil},
        )
        exec_sql("DELETE FROM articles WHERE article=:a;", {"a": old_article})

        # Met à jour mouvements
        exec_sql(
            """
            UPDATE mouvements
            SET article = :newa,
                designation = :newd
            WHERE article = :olda;
            """,
            {"newa": new_article, "newd": new_designation, "olda": old_article},
        )
    else:
        # Juste designation
        exec_sql(
            "UPDATE articles SET designation=:d WHERE article=:a;",
            {"d": new_designation, "a": old_article},
        )
        exec_sql(
            "UPDATE mouvements SET designation=:d WHERE article=:a;",
            {"d": new_designation, "a": old_article},
        )


def add_adresse_if_needed(nom: str) -> None:
    nom = (nom or "").strip()
    if not nom:
        return
    exec_sql(
        "INSERT INTO adresses(nom) VALUES (:n) ON CONFLICT (nom) DO NOTHING;",
        {"n": nom},
    )


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
    """
    Règles stock/garantie :
      - STOCK + ENTREE  => stock += qty
      - STOCK + SORTIE  => stock -= qty
      - GARANTIE + ENTREE => garantie += qty ET stock -= qty  (deduction automatique du stock)
      - GARANTIE + SORTIE => garantie -= qty
    """
    # Charge état
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
            # on met en garantie => garantie +, stock -
            new_garantie += qty
            new_stock -= qty
        else:
            new_garantie -= qty

    if new_stock < 0:
        raise ValueError("Stock insuffisant pour cette opération.")
    if new_garantie < 0:
        raise ValueError("Garantie insuffisante pour cette opération.")

    # Update article
    exec_sql(
        "UPDATE articles SET stock=:s, garantie=:g WHERE article=:a;",
        {"s": new_stock, "g": new_garantie, "a": article},
    )

    # Insert historique
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


def update_article_seuil(article: str, seuil: int | None) -> None:
    exec_sql(
        "UPDATE articles SET seuil_commande=:s WHERE article=:a;",
        {"s": seuil, "a": article},
    )


# =========================================================
# UI HELPERS (style)
# =========================================================
def historise_display(df_hist: pd.DataFrame) -> pd.DataFrame:
    df = df_hist.copy()
    # Remarque + icône
    df["remarque"] = df["commentaire"].fillna("").astype(str)
    df["remarque"] = df["remarque"].apply(lambda x: f"⚠️ {x}" if x.strip() else "")
    df = df.drop(columns=["commentaire"], errors="ignore")
    # Colonnes affichées (plus lisible)
    df = df.rename(
        columns={
            "type_mvt": "type",
            "quantite": "qté",
        }
    )
    return df


def style_remarque_red(df_display: pd.DataFrame):
    # Colorer la colonne "remarque" en rouge si non vide
    def color_remarque(val: str):
        if isinstance(val, str) and val.strip():
            return "color: red; font-weight: 700;"
        return ""

    if "remarque" not in df_display.columns:
        return None

    return df_display.style.applymap(color_remarque, subset=["remarque"])


# =========================================================
# APP
# =========================================================
st.title(APP_TITLE)

tab1, tab2, tab3 = st.tabs(["➕ Mouvement", "📦 Stock / Garantie / Gestion", "📍 Adresses"])

# ---------------------------------------------------------
# TAB 1 — MOUVEMENT
# ---------------------------------------------------------
with tab1:
    st.subheader("Ajouter un mouvement")

    articles_df = get_articles()
    known_articles = [] if articles_df.empty else articles_df["article"].astype(str).tolist()

    colL, colM, colR = st.columns([1.2, 1.2, 1.2])

    with colL:
        d = st.date_input("Date", value=date.today())
        article_input = st.text_input("Numéro d'article", placeholder="Ex: 155082")

    with colM:
        emplacement = st.selectbox("Emplacement", ["STOCK", "GARANTIE"])
        type_mvt = st.selectbox("Type", ["ENTREE", "SORTIE"])
        qty = st.number_input("Quantité", min_value=1, max_value=10_000, value=1, step=1)

    # Désignation auto si article existe, sinon champ "1ère fois"
    designation_auto = get_designation(article_input.strip()) if article_input.strip() else ""
    with colL:
        if designation_auto:
            st.text_input("Désignation (auto)", value=designation_auto, disabled=True)
            designation = designation_auto
        else:
            designation = st.text_input("Désignation (1ère fois)", placeholder="Ex: Sonde O2")

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
                art = article_input.strip()
                if not art:
                    raise ValueError("Numéro d'article obligatoire.")
                if not designation.strip():
                    # si article existe, designation_auto aurait rempli. Sinon erreur.
                    raise ValueError("Désignation obligatoire (au moins la première fois).")

                # créer / maj article
                add_or_update_article(art, designation.strip())

                # créer adresse si besoin
                add_adresse_if_needed(adresse or "")

                # applique mouvement
                apply_movement(
                    d=d,
                    article=art,
                    designation=designation.strip(),
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

    st.divider()
    st.subheader("✏️ Modifier numéro / désignation (en bas de Mouvement)")

    articles_df = get_articles()
    if articles_df.empty:
        st.info("Aucun article à modifier.")
    else:
        choix_art = st.selectbox("Choisir l'article", articles_df["article"].astype(str).tolist())
        current_design = get_designation(choix_art)

        new_num = st.text_input("Nouveau numéro", value=choix_art)
        new_design = st.text_input("Nouvelle désignation", value=current_design)

        if st.button("💾 Sauvegarder les modifications"):
            try:
                update_article_number_and_designation(
                    old_article=choix_art,
                    new_article=new_num.strip(),
                    new_designation=new_design.strip(),
                )
                st.success("Modification enregistrée ✅")
                st.rerun()
            except Exception as e:
                st.error(str(e))


# ---------------------------------------------------------
# TAB 2 — STOCK / GARANTIE / GESTION
# ---------------------------------------------------------
with tab2:
    articles_df = get_articles()

    # --- Barre de recherche (article)
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

        # ---- Edition du seuil par pièce dans le tableau
        st.caption("Tu peux modifier la colonne **Seuil pièce** pour chaque article (laisser vide = utiliser le seuil global).")

        df_editor = df_view[["article", "designation", "stock", "seuil_commande"]].rename(
            columns={"seuil_commande": "Seuil pièce"}
        )

        edited = st.data_editor(
            df_editor,
            use_container_width=True,
            hide_index=True,
            column_config={
                "article": st.column_config.TextColumn("Article", disabled=True),
                "designation": st.column_config.TextColumn("Désignation", disabled=True),
                "stock": st.column_config.NumberColumn("Stock", disabled=True),
                "Seuil pièce": st.column_config.NumberColumn(
                    "Seuil pièce",
                    help="Si Stock ≤ ce seuil => apparaît dans Pièces à commander. Vide = seuil global.",
                    min_value=0,
                    step=1,
                ),
            },
            key="editor_seuil_piece",
        )

        # Sauvegarde des seuils modifiés
        # (On compare l'ancienne valeur et la nouvelle)
        try:
            old_map = dict(
                zip(
                    df_view["article"].astype(str),
                    df_view["seuil_commande"].where(~df_view["seuil_commande"].isna(), None).tolist(),
                )
            )
            new_map = dict(
                zip(
                    edited["article"].astype(str),
                    edited["Seuil pièce"].where(~edited["Seuil pièce"].isna(), None).tolist(),
                )
            )

            changes = []
            for art, new_val in new_map.items():
                old_val = old_map.get(art, None)
                if old_val != new_val:
                    changes.append((art, new_val))

            if changes:
                for art, val in changes:
                    if val is None:
                        update_article_seuil(art, None)
                    else:
                        update_article_seuil(art, int(val))
                st.success("Seuils par pièce mis à jour ✅")
                st.rerun()
        except Exception:
            pass

    st.divider()

    # ---- Seuil global + Pièces à commander + Garantie
    colA, colB = st.columns(2)

    with colA:
        st.subheader("📦 Pièces à commander")

        # Seuil global (sert si pièce n'a pas de seuil)
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
                    a_commander[["article", "designation", "stock", "seuil_effectif"]].rename(
                        columns={"seuil_effectif": "Seuil utilisé"}
                    ),
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

    # ---- Historique
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

    # ---- Supprimer un article (tout en bas)
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

        # liste des pièces utilisées + dates (sorties stock avec adresse)
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
