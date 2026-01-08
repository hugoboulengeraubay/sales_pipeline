# Sales Pipeline - Databricks

## But du projet
Pipeline de traitement des ventes en **3 couches** :
- **Bronze** : ingestion des CSV bruts depuis DBFS.
- **Silver** : nettoyage et harmonisation des données.
- **Gold** : agrégations et calculs pour analyse (CA, classement produits).

## Lancement

### Pipeline complet
Le fichier `pipeline.py` dans le dossier pipeline permet de lancer le pipeline entier.

### Tests
Le fichier `launch_tests.ipynb` permet de lancer les tests.

## CI/CD avec GitHub Actions

Le pipeline CI/CD est déclenché sur :

- Push sur la branche `main`
- Création de tags `v*`

### Étapes principales

1. **Build**
   - Checkout du code
   - Installation de Python 3.12 et des dépendances
   - Lancement des tests (`pytest`)
   - Build du package Python
   - Upload des artefacts (`dist/`) pour publication

2. **Publish**
   - Déclenché seulement sur tags (`v*`)
   - Téléchargement des artefacts du build
   - Publication sur PyPI

Le workflow complet est défini dans `.github/workflows/ci_cd.yml`.
