# Sales Pipeline - Databricks

## But du projet
Pipeline de traitement des ventes en **3 couches** :
- **Bronze** : ingestion des CSV bruts depuis DBFS.
- **Silver** : nettoyage et harmonisation des données.
- **Gold** : agrégations et calculs pour analyse (CA, classement produits).

## Lancement

### Pipeline complet
Le fichier `main.py` permet de lancer le pipeline entier.

### Tests
Le fichier `launch_tests.ipynb` permet de lancer les tests.
