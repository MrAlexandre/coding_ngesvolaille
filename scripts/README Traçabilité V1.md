# **Traçabilité du pipeline V1 : rôles et fonctionnement des tables runs, journal\_etapes et suivi\_depots**



## 

## Introduction

Le pipeline de traitement des données repose sur trois tables principales stockées dans la base SQLite "registry.sqlite".

Ces tables constituent le cœur du système de traçabilité et permettent de suivre :



les exécutions du pipeline ;

les actions techniques réalisées ;

le cycle de vie des fichiers traités.



Les trois tables sont :



runs

journal\_etapes

suivi\_depots



Elles correspondent à trois niveaux complémentaires de lecture : global, technique et métier.



## 

## Table runs — Suivi des exécutions



### Rôle



La table "runs" enregistre chaque exécution complète du pipeline (appelée aussi run ou micro-lot).



Chaque ligne correspond à un lancement du pipeline.



Informations contenues :

* ingestion\_id : identifiant unique du run
* started\_at : date et heure de début
* ended\_at : date et heure de fin
* status : statut global (SUCCESS, FAILED, etc.)
* files\_detected : nombre de fichiers détectés
* files\_processed : nombre de fichiers traités
* files\_failed : nombre de fichiers en erreur

### 

### Utilité

* suivre l’activité globale du pipeline
* identifier rapidement les exécutions réussies ou en échec
* mesurer le volume de données traité



### Interprétation



Cette table correspond à un journal des exécutions, équivalent à un tableau de bord.



## 

## Table journal\_etapes — Journal détaillé des traitements



### Rôle



La table "journal\_etapes" enregistre toutes les actions réalisées par le pipeline, étape par étape.



Chaque ligne correspond à un événement technique.



Informations contenues :

* event\_id : identifiant de l’événement
* ingestion\_id : identifiant du run
* file\_id : identifiant du fichier (si applicable)
* step\_code : étape du pipeline
* action\_type : type d’action réalisée
* status : statut (OK, ERROR, etc.)
* event\_time : horodatage
* message : description de l’action
* details\_json : informations complémentaires



### Utilité

* tracer précisément toutes les opérations
* comprendre le déroulement du pipeline
* diagnostiquer les erreurs
* analyser les comportements anormaux



### Interprétation



Cette table correspond à un journal technique détaillé, comparable à des logs structurés.



## 

## Table suivi\_depots — Suivi des fichiers

### Rôle

La table "suivi\_depots" enregistre chaque fichier détecté et suivi par le pipeline.



Chaque ligne correspond à un fichier.



Informations contenues :

* file\_id : identifiant du fichier
* ingestion\_id : identifiant du run
* source\_filename : nom du fichier
* source\_path : chemin complet du fichier
* partner : origine du fichier
* extension : type de fichier
* file\_size\_bytes : taille du fichier
* detected\_at : date de détection
* sha256 : empreinte du fichier
* global\_status : statut global du fichier
* archive\_path : chemin d’archivage



### Utilité

* suivre le cycle de vie des fichiers
* éviter les doublons (idempotence)
* connaître le statut de chaque fichier
* retrouver un fichier dans le système



## Interprétation

Cette table correspond à un registre métier des fichiers, comparable à un système de suivi logistique.



## 

## Articulation des tables

Les trois tables sont liées entre elles via les identifiants ingestion\_id et file\_id.



Organisation logique :

* runs décrit une exécution du pipeline
* journal\_etapes décrit les actions de cette exécution
* suivi\_depots décrit les fichiers concernés



Cette structure permet :

* d’analyser un run en détail
* de suivre le traitement d’un fichier
* d’assurer une traçabilité complète



## Résumé

* runs : quand la pipeline a tourné et avec quel résultat
* journal\_etapes : ce que la pipeline a fait, étape par étape
* suivi\_depots : ce qui est arrivé à chaque fichier



## Remise à zéro du pipeline



Pour repartir d’un état initial, il est nécessaire de vider les trois tables :



DELETE FROM runs;

DELETE FROM journal\_etapes;

DELETE FROM suivi\_depots;



Cette opération supprime l’historique sans supprimer la structure des tables.



## 

## Conclusion

La structuration en trois tables permet :

* une vision globale (runs)
* une vision technique détaillée (journal\_etapes)
* une vision métier par fichier (suivi\_depots)



Cette organisation garantit la traçabilité, la compréhension et la robustesse du pipeline de traitement des données.

