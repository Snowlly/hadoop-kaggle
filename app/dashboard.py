import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException

SQL_CATEGORIES = """
WITH categories AS (
    SELECT categorie_produit, COUNT(*) AS nb_commandes,
           SUM(a_un_commentaire) AS nb_commentaires,
           AVG(CAST(a_un_commentaire AS DOUBLE)) AS taux_commentaire,
           SUM(prix) AS total_prix_brl,
           PERCENTILE_APPROX(prix, ARRAY(0.25, 0.5, 0.75), 10000) AS quantiles_prix,
           PERCENTILE_APPROX(delai_livraison_jours, 0.5, 10000) AS delai_median_jours
    FROM v_silver GROUP BY categorie_produit
)
SELECT categorie_produit, nb_commandes, nb_commentaires, taux_commentaire, total_prix_brl,
       quantiles_prix[1] AS prix_median_brl,
       quantiles_prix[2] - quantiles_prix[0] AS prix_iqr_brl,
       delai_median_jours,
       DENSE_RANK() OVER (ORDER BY nb_commentaires DESC) AS rang_volume_commentaires,
       SUM(nb_commentaires) OVER (
           ORDER BY nb_commentaires DESC, categorie_produit
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) / NULLIF(SUM(nb_commentaires) OVER (), 0) AS part_commentaires_cumulee,
       SUM(nb_commandes) OVER (
           ORDER BY nb_commentaires DESC, categorie_produit
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) / SUM(nb_commandes) OVER () AS part_commandes_cumulee,
       nb_commandes < 100 AS effectif_faible
FROM categories
"""

SQL_MENSUEL = """
WITH mensuel AS (
    SELECT TRUNC(TO_DATE(date_livraison), 'MM') AS periode,
           COUNT(*) AS nb_commandes, SUM(a_un_commentaire) AS nb_commentaires,
           PERCENTILE_APPROX(prix, 0.5, 10000) AS prix_median_brl,
           PERCENTILE_APPROX(delai_livraison_jours, 0.5, 10000) AS delai_median_jours
    FROM v_silver GROUP BY TRUNC(TO_DATE(date_livraison), 'MM')
), calendrier AS (
    SELECT EXPLODE(SEQUENCE(MIN(periode), MAX(periode), INTERVAL 1 MONTH)) AS periode
    FROM mensuel
), serie AS (
    SELECT c.periode, COALESCE(m.nb_commandes, 0) AS nb_commandes,
           COALESCE(m.nb_commentaires, 0) AS nb_commentaires,
           m.nb_commentaires / NULLIF(m.nb_commandes, 0) AS taux_commentaire,
           m.prix_median_brl, m.delai_median_jours
    FROM calendrier c LEFT JOIN mensuel m ON c.periode = m.periode
), decalee AS (
    SELECT *,
           LAG(nb_commandes, 1) OVER (ORDER BY periode) AS commandes_mois_precedent,
           LAG(nb_commentaires, 1) OVER (ORDER BY periode) AS commentaires_mois_precedent,
           LAG(taux_commentaire, 1) OVER (ORDER BY periode) AS taux_mois_precedent
    FROM serie
)
SELECT *,
       100.0 * (nb_commentaires - commentaires_mois_precedent)
           / NULLIF(commentaires_mois_precedent, 0) AS evolution_commentaires_pct,
       100.0 * (taux_commentaire - taux_mois_precedent) AS variation_taux_points,
       nb_commandes < 100 AS effectif_faible
FROM decalee
"""

SQL_PRIX_RETARD = """
WITH tranches AS (
    SELECT *,
           CASE WHEN prix <= 20 THEN 0 WHEN prix <= 50 THEN 1
                WHEN prix <= 100 THEN 2 WHEN prix <= 200 THEN 3 ELSE 4 END AS ordre_prix,
           CASE WHEN retard_jours = 0 THEN 0 WHEN retard_jours <= 3 THEN 1
                WHEN retard_jours <= 7 THEN 2 ELSE 3 END AS ordre_retard
    FROM v_silver
), groupes AS (
    SELECT ordre_prix, ordre_retard, COUNT(*) AS nb_commandes,
           SUM(a_un_commentaire) AS nb_commentaires,
           AVG(CAST(a_un_commentaire AS DOUBLE)) AS taux_commentaire,
           PERCENTILE_APPROX(prix, 0.5, 10000) AS prix_median_brl,
           PERCENTILE_APPROX(delai_livraison_jours, 0.5, 10000) AS delai_median_jours
    FROM tranches GROUP BY ordre_prix, ordre_retard
), classes AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ordre_retard
               ORDER BY CASE WHEN nb_commandes >= 100 THEN 0 ELSE 1 END,
                        taux_commentaire DESC, nb_commandes DESC, ordre_prix
           ) AS rang_taux_par_retard
    FROM groupes
)
SELECT *,
       CASE ordre_prix WHEN 0 THEN '0 à 20 BRL' WHEN 1 THEN ']20, 50] BRL'
            WHEN 2 THEN ']50, 100] BRL' WHEN 3 THEN ']100, 200] BRL'
            ELSE 'Plus de 200 BRL' END AS tranche_prix,
       CASE ordre_retard WHEN 0 THEN 'À temps' WHEN 1 THEN '1 à 3 jours'
            WHEN 2 THEN '4 à 7 jours' ELSE '8 jours et plus' END AS tranche_retard,
       nb_commandes < 100 AS effectif_faible
FROM classes
"""

SQL_SEGMENTS = """
SELECT categorie_produit, COUNT(*) AS nb_commandes,
       SUM(a_un_commentaire) AS nb_commentaires,
       AVG(CAST(a_un_commentaire AS DOUBLE)) AS taux_commentaire,
       PERCENTILE_APPROX(prix, 0.5, 10000) AS prix_median,
       PERCENTILE_APPROX(frais_livraison, 0.5, 10000) AS frais_medians,
       PERCENTILE_APPROX(delai_livraison_jours, 0.5, 10000) AS delai_median,
       AVG(CAST(livraison_en_retard AS INT)) AS taux_retard
FROM v_silver_j4 GROUP BY categorie_produit
HAVING COUNT(*) >= 100
"""

st.set_page_config(page_title="Commentaires après livraison · Olist", layout="wide")
st.title("Commentaires écrits après livraison")
st.caption("Olist · commandes livrées à un seul article · prix en réals brésiliens (BRL)")


@st.cache_resource
def get_spark():
    return (
        SparkSession.builder.appName("StreamlitGoldReader")
        .config("spark.driver.memory", "512m")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@st.cache_data(ttl=60)
def load_gold_table(name):
    return get_spark().read.format("delta").load(f"/lakehouse/gold/{name}").toPandas()


def expliquer(sql, justification):
    with st.expander("Requête SQL & Justification statistique"):
        st.code(sql, language="sql")
        st.write(justification)


try:
    categories = load_gold_table("olist_commentaires_categories")
    mensuel = load_gold_table("olist_commentaires_mensuels")
    prix_retard = load_gold_table("olist_commentaires_prix_retard")
    segments = load_gold_table("olist_segments_categories")
    tests = load_gold_table("olist_tests_statistiques")
    evaluation = load_gold_table("olist_modele_evaluation")
    calibration = load_gold_table("olist_modele_calibration")
    predictions = load_gold_table("olist_predictions_commentaires")
except AnalysisException:
    st.info("Exécute les notebooks J3 et J4 pour créer toutes les tables Gold, puis recharge cette page.")
    st.stop()

total = int(categories.nb_commandes.sum())
comments = int(categories.nb_commentaires.sum())
if total == 0:
    st.info("Les tables Gold sont vides. Exécute J3 et J4 avec une table Silver renseignée.")
    st.stop()
left, middle, right = st.columns(3)
left.metric("Commandes analysées", f"{total:,}")
middle.metric("Commentaires après livraison", f"{comments:,}")
right.metric("Taux observé", f"{comments / total:.1%}")

exploration_tab, segments_tab, prediction_tab = st.tabs(["Exploration", "Segments et tests", "Prédiction"])

with exploration_tab:
    st.subheader("Catégories de produits")
    threshold = st.slider("Nombre minimal de commandes par catégorie", min_value=100, max_value=1000, value=500, step=100)
    available = categories.loc[categories.nb_commandes >= threshold].sort_values("nb_commandes", ascending=False)
    selected = st.multiselect(
        "Catégories à comparer", available.categorie_produit.tolist(),
        default=available.categorie_produit.head(10).tolist(),
    )
    filtered = available.loc[available.categorie_produit.isin(selected)].sort_values("taux_commentaire")
    if len(filtered):
        fig = px.bar(
            filtered, x="taux_commentaire", y="categorie_produit", orientation="h",
            hover_data={"nb_commandes": True, "prix_median_brl": ":.2f"},
            labels={"taux_commentaire": "Commentaires après livraison", "categorie_produit": "Catégorie",
                    "nb_commandes": "Commandes", "prix_median_brl": "Prix médian (BRL)"},
            color_discrete_sequence=["#087c70"],
        )
        fig.update_xaxes(tickformat=".0%")
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("Sélectionne au moins une catégorie pour afficher le graphique.")
    expliquer(
        SQL_CATEGORIES,
        "La médiane et l’IQR décrivent les prix asymétriques. Le taux est affiché avec son effectif pour comparer des catégories de volumes différents. Les filtres concernent ce graphique.",
    )

    st.subheader("Évolution par mois de livraison")
    show_small = st.checkbox("Inclure les mois de moins de 100 commandes", value=False)
    months = mensuel if show_small else mensuel.loc[~mensuel.effectif_faible]
    months = months.sort_values("periode")
    fig = px.line(
        months, x="periode", y="taux_commentaire", markers=True,
        hover_data={"nb_commandes": True, "nb_commentaires": True, "variation_taux_points": ":.2f"},
        labels={"periode": "Mois de livraison", "taux_commentaire": "Commentaires après livraison",
                "nb_commandes": "Commandes", "nb_commentaires": "Commentaires", "variation_taux_points": "Variation (points)"},
    )
    fig.update_yaxes(tickformat=".0%")
    st.plotly_chart(fig, width="stretch")
    expliquer(
        SQL_MENSUEL,
        "La série est triée chronologiquement et LAG mesure la variation par rapport au mois précédent. La période désigne la cohorte de livraison. Les commandes de fin de période ont moins de recul pour recevoir un commentaire.",
    )

    st.subheader("Prix de l’article et retard")
    price_order = prix_retard.sort_values("ordre_prix").tranche_prix.drop_duplicates().tolist()
    delay_order = prix_retard.sort_values("ordre_retard").tranche_retard.drop_duplicates().tolist()
    usable = prix_retard.copy()
    usable.loc[usable.effectif_faible, "taux_commentaire"] = float("nan")
    matrix = usable.pivot(index="tranche_prix", columns="tranche_retard", values="taux_commentaire").reindex(index=price_order, columns=delay_order)
    fig = px.imshow(
        matrix, text_auto=".1%", zmin=0, zmax=1, aspect="auto", color_continuous_scale="YlGnBu",
        labels={"x": "Retard", "y": "Prix de l’article", "color": "Commentaires"},
    )
    fig.update_coloraxes(colorbar_tickformat=".0%")
    st.plotly_chart(fig, width="stretch")
    st.caption("Les groupes de moins de 100 commandes sont masqués. La cible compte les messages écrits à partir de la livraison réelle.")
    expliquer(
        SQL_PRIX_RETARD,
        "Le croisement compare les taux à retard identique selon le prix. ROW_NUMBER classe les tranches au sein d’un même niveau de retard. Les écarts restent des associations sur les données historiques.",
    )

with segments_tab:
    st.subheader("Segments de catégories")
    selected_segments = st.multiselect(
        "Segments à afficher", sorted(segments.segment.unique()), default=sorted(segments.segment.unique()),
    )
    visible = segments.loc[segments.segment.isin(selected_segments)]
    if len(visible):
        fig = px.scatter(
            visible, x="prix_median", y="taux_commentaire", color="segment", size="nb_commandes",
            hover_name="categorie_produit", log_x=True,
            hover_data={"delai_median": True, "taux_retard": ":.1%"},
            labels={"prix_median": "Prix médian (BRL)", "taux_commentaire": "Commentaires après livraison",
                    "segment": "Segment", "nb_commandes": "Commandes"},
        )
        fig.update_yaxes(tickformat=".0%")
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("Sélectionne un segment.")
    expliquer(
        SQL_SEGMENTS,
        "K-Means utilise quatre variables de profil. Les montants et le délai sont transformés par log1p puis standardisés. Le taux de commentaire sert à interpréter les groupes obtenus.",
    )
    st.subheader("Tests d’hypothèses")
    st.dataframe(
        tests[["hypothese", "test", "p_holm", "effet", "valeur_effet", "decision"]],
        hide_index=True, width="stretch",
    )
    st.caption("Correction de Holm pour deux tests au seuil de 5 %. La taille de l’effet accompagne la p-value.")

with prediction_tab:
    st.subheader("Probabilités sur des commandes plus récentes")
    st.write(
        "La régression logistique est entraînée sur les 80 % de commandes les plus anciennes. "
        "Les 20 % les plus récentes servent au test. Elle utilise le prix, le retard et leur interaction, "
        "la catégorie, le poids, les frais et le délai de livraison."
    )
    st.dataframe(evaluation, hide_index=True, width="stretch")
    st.caption("AUC et précision moyenne augmentent avec la qualité du classement. Brier et log-loss diminuent avec la qualité probabiliste.")
    calibration = calibration.sort_values("groupe_probabilite")
    fig = px.line(
        calibration, x="probabilite_moyenne", y="frequence_observee", markers=True,
        hover_data=["nb_commandes"],
        labels={"probabilite_moyenne": "Probabilité prédite moyenne", "frequence_observee": "Fréquence observée"},
    )
    fig.add_trace(go.Scatter(x=[0, 1], y=[0, 1], mode="lines", name="Accord idéal", line={"dash": "dash", "color": "#8297aa"}))
    fig.update_xaxes(range=[0, 1], tickformat=".0%")
    fig.update_yaxes(range=[0, 1], tickformat=".0%")
    st.plotly_chart(fig, width="stretch")
    expliquer(
        "SELECT groupe_probabilite, nb_commandes, probabilite_moyenne, frequence_observee\nFROM delta.`/lakehouse/gold/olist_modele_calibration`\nORDER BY groupe_probabilite",
        "Chaque groupe rassemble des probabilités prédites proches. L’imputation et l’encodage sont appris sur l’entraînement. Les résultats sont une évaluation rétrospective de la cible historique.",
    )
    chosen_category = st.selectbox(
        "Catégorie des commandes de test", ["Toutes", *sorted(predictions.categorie_produit.unique())],
    )
    chosen = predictions if chosen_category == "Toutes" else predictions.loc[predictions.categorie_produit == chosen_category]
    st.dataframe(
        chosen.sort_values(["probabilite_commentaire", "order_id"], ascending=[False, True]).head(100),
        hide_index=True, width="stretch",
        column_config={
            "probabilite_commentaire": st.column_config.NumberColumn("Probabilité estimée", format="percent"),
            "a_un_commentaire": st.column_config.NumberColumn("Commentaire observé (0/1)"),
        },
    )
    st.caption("Affichage des 100 probabilités les plus élevées du filtre. La durée de suivi varie dans le jeu historique ; un usage réel demande un horizon de réponse commun.")
