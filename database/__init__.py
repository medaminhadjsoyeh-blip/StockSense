"""Couche d'accès MongoDB de StockSense (Étudiant 3).

Modules :
- config     : paramètres de connexion (variables d'environnement).
- schema     : contrat de données partagé (collections, champs, index, validateurs).
- client     : création du client et de la base.
- repository : fonctions d'insertion, de mise à jour (upsert) et de lecture.
- metrics    : calculs purs réutilisables (couverture de stock, KPI).
"""
