#!/bin/bash
# download the metadata file from French Gov 
# URL: https://www.data.gouv.fr/datasets/metadonnees-des-discours-publics-de-vie-publique-fr?utm_source=chatgpt.com
mkdir dataset
mkdir logs
curl -o dataset/vp_discours.json https://echanges.dila.gouv.fr/OPENDATA/DISCOURS_PUBLICS/vp_discours.json