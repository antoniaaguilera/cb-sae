# ----------------------------------------- #
# ---- VALIDACIONES DE SALIDA SAE 2022 ---- #
# ----------------------------------------- #

# --- cargar paquetes ---  #
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import scipy.stats as stats
import numpy as np
import math
import json
import pdfkit

import psycopg2

host='localhost'
user = 'antoniaaguilera'
password='Papaitopostgres4!'
port='5432'
database = 'sae_consolidado_uchile'

db_connection = psycopg2.connect(database=database,
    host=host,
    user=user,
    password=password,
    port=port)

# --- 
query_ordenamiento = f'select detalle_ordenamiento.id_postulante, detalle_ordenamiento.id_ordenamiento, detalle_ordenamiento.numero, postulante.sexo, postulante.prioritario, postulante.etiqueta_nivel, ordenamiento.rbd, ordenamiento.cod_curso, ordenamiento.ordenamiento from {database}.establecimiento.detalle_ordenamiento as detalle_ordenamiento left join {database}.postulacion.postulante postulante on postulante.id_postulante = detalle_ordenamiento.id_postulante left join {database}.establecimiento.ordenamiento ordenamiento on ordenamiento.id_ordenamiento = detalle_ordenamiento.id_ordenamiento ;'
cursor = db_connection.cursor()
cursor.execute(query_ordenamiento)
df_ordenamiento = pd.DataFrame(cursor.fetchall())
df_ordenamiento.columns = ['id_postulante', 'id_ordenamiento', 'numero', 'sexo','prioritario','etiqueta_nivel', 'rbd', 'cod_curso', 'ordenamiento']

ord1_alpha = df_ordenamiento[df_ordenamiento['ordenamiento']=="ALPHA"].reset_index()
ord1_beta  = df_ordenamiento[df_ordenamiento['ordenamiento']=="BETA"].reset_index()
# posición relativa de cada estudiante
ord1_alpha['rank'] = ord1_alpha.groupby(['rbd', 'cod_curso'])['numero'].rank(pct=True)
ord1_beta['rank']  = ord1_beta.groupby(['rbd', 'cod_curso'])['numero'].rank(pct=True)
ord1_alpha['mean_byapp'] = ord1_alpha.groupby(['id_postulante'])['rank'].transform('mean')
ord1_beta['mean_byapp']  = ord1_beta.groupby(['id_postulante'])['rank'].transform('mean')
# correlacion enter alpha y beta
corr1 = ord1_alpha['mean_byapp'].corr(ord1_beta['mean_byapp'], method='pearson')

# plot1
fig, axes =plt.subplots(1,2)
ord1_alpha['mean_byapp'].plot.hist(bins=100,ax=axes[0]).set_title('alpha')
ord1_beta['mean_byapp'].plot.hist(bins=100, ax=axes[1]).set_title('beta')
plt.savefig('distr_ord.png')
plt.clf()
# plot2
fig, axes =plt.subplots(1,2)
women=ord1_alpha['mean_byapp'][ord1_alpha['sexo'] == 'F']
men=ord1_alpha['mean_byapp'][ord1_alpha['sexo'] == 'M']
women.plot.hist(bins=100,ax=axes[0]).set_title('mujeres')
men.plot.hist(bins=100,ax=axes[1]).set_title('hombres')
plt.savefig('distr_bysex.png')

#densidad por curso 
sns.displot(data = ord1_alpha, x='mean_byapp', hue='etiqueta_nivel',kind='kde').set_axis_labels('Posición relativa promedio (alpha)', 'Densidad')
plt.savefig('distr_bynivel_alpha.png')
sns.displot(data = ord1_beta, x='mean_byapp', hue='etiqueta_nivel',kind='kde').set_axis_labels('Posición relativa promedio (beta)', 'Densidad')
plt.savefig('distr_bynivel_beta.png')

# test de medias por grupo:
# -- por genero -- #
pval_gen_alpha = stats.ttest_ind(ord1_alpha['mean_byapp'][ord1_alpha['sexo'] == 'M'], ord1_alpha['mean_byapp'][ord1_alpha['sexo'] == 'F']).pvalue
pval_gen_beta = stats.ttest_ind(ord1_beta['mean_byapp'][ord1_beta['sexo'] == 'M'], ord1_beta['mean_byapp'][ord1_beta['sexo'] == 'F']).pvalue
# -- por prioritario -- #
pval_prio_alpha = stats.ttest_ind(ord1_alpha['mean_byapp'][ord1_alpha['prioritario'] == True], ord1_alpha['mean_byapp'][ord1_alpha['prioritario'] == False]).pvalue
pval_prio_beta = stats.ttest_ind(ord1_beta['mean_byapp'][ord1_beta['prioritario'] == True], ord1_beta['mean_byapp'][ord1_beta['prioritario'] == False]).pvalue

# --- hermanos --- #
query_ord_hermanos = f'select detalle_ordenamiento.id_postulante, detalle_ordenamiento.id_ordenamiento, detalle_ordenamiento.numero, ordenamiento.rbd, ordenamiento.cod_curso, ordenamiento.ordenamiento, hermano.id_hermano, bloque.id_bloque from {database}.establecimiento.detalle_ordenamiento as detalle_ordenamiento left join {database}.establecimiento.ordenamiento ordenamiento on ordenamiento.id_ordenamiento = detalle_ordenamiento.id_ordenamiento left join {database}.postulacion.hermano hermano on hermano.id_postulante = detalle_ordenamiento.id_postulante left join {database}.postulacion.bloque bloque on bloque.id_postulante = detalle_ordenamiento.id_postulante ;'
cursor = db_connection.cursor()
cursor.execute(query_ord_hermanos)
df_ord_hermanos = pd.DataFrame(cursor.fetchall())
df_ord_hermanos.columns = ['id_postulante', 'id_ordenamiento', 'numero', 'rbd', 'cod_curso', 'ordenamiento', 'id_hermano', 'id_bloque']

ord2_alpha = df_ord_hermanos[df_ord_hermanos['ordenamiento']=="ALPHA"].reset_index()
ord2_beta  = df_ord_hermanos[df_ord_hermanos['ordenamiento']=="BETA"].reset_index()

# posición relativa de cada estudiante
ord2_alpha['rank'] = ord2_alpha.groupby(['rbd', 'cod_curso'])['numero'].rank(pct=True)
ord2_beta['rank']  = ord2_beta.groupby(['rbd', 'cod_curso'])['numero'].rank(pct=True)
ord2_alpha['mean_byapp'] = ord2_alpha.groupby(['id_postulante'])['rank'].transform('mean')
ord2_beta['mean_byapp']  = ord2_beta.groupby(['id_postulante'])['rank'].transform('mean')
# fillna
ord2_alpha['id_hermano'] = ord2_alpha['id_hermano'].fillna(value='NO')
ord2_beta['id_hermano']  = ord2_beta['id_hermano'].fillna(value='NO')
ord2_alpha['id_bloque']  = ord2_alpha['id_bloque'].fillna(value='NO')
ord2_beta['id_bloque']   = ord2_beta['id_bloque'].fillna(value='NO')

# test de medias por grupo:
# -- por hermano -- #
pval_hermano_alpha = stats.ttest_ind(ord2_alpha['mean_byapp'][ord2_alpha['id_hermano'] != 'NO'], ord2_alpha['mean_byapp'][ord2_alpha['id_hermano'] == 'NO'])
pval_hermano_alpha = pval_hermano_alpha.pvalue
pval_hermano_beta = stats.ttest_ind(ord2_beta['mean_byapp'][ord2_beta['id_hermano'] != 'NO'], ord2_beta['mean_byapp'][ord2_beta['id_hermano'] == 'NO'])
pval_hermano_beta = pval_hermano_beta.pvalue

# -- por bloque -- #
pval_bloque_alpha = stats.ttest_ind(ord2_alpha['mean_byapp'][ord2_alpha['id_bloque'] != 'NO'], ord2_alpha['mean_byapp'][ord2_alpha['id_bloque'] == 'NO'])
pval_bloque_alpha = pval_bloque_alpha.pvalue
pval_bloque_beta = stats.ttest_ind(ord2_beta['mean_byapp'][ord2_beta['id_bloque'] != 'NO'], ord2_beta['mean_byapp'][ord2_beta['id_bloque'] == 'NO'])
pval_bloque_beta = pval_bloque_beta.pvalue
# generar tablas para mostrar
index1 = ['Género', 'Hermanos', 'Bloques', 'Prioritarios']
pvals_alpha  = [pval_gen_alpha, pval_hermano_alpha, pval_bloque_alpha, pval_prio_alpha]
pvals_beta   = [pval_gen_beta, pval_hermano_beta, pval_bloque_beta, pval_prio_beta]
df_pvals = pd.DataFrame(list(zip(pvals_alpha, pvals_beta)), index=index1, columns=['pval para alpha', 'pval para beta'])

page_title_text='Validaciones SAE 2022: Período Principal'
title_text = 'Validación ordenamiento aleatorio'

html = f'''
    <html>
        <head>
            <title>{page_title_text}</title>
        </head>
        <body>
            <h1>{title_text}</h1>
            <p>{f'Para cada postulante, se obtiene la posición relativa en la queda ordenado en cada una de sus preferencias en base a la variable detalle_ordenamiento.numero. La siguiente figura muestra la distribución del promedio de la posición relativa para cada estudiante. Se espera que esta distribución siga una distribución normal para cada uno de los ordenamientos (alpha y beta). Adicional se obtiene la correlación entre la serie ALPHA y BETA, con un valor de {corr1}. '}</p>
            <img src='distr_ord.png' />
            <img src='distr_bysex.png'/>
            <p>{'La siguiente figura muestra a demás la densidad de la posición relativa promedio de cada estudiante según su nivel. Si existe aleatoriedad se espera que esta se asemeje a una normal.'}<p>
            <img src='distr_bynivel_alpha.png' />
            <img src='distr_bynivel_beta.png' />
            <p>{'La siguiente tabla muestra los test de medias sobre el promedio de la posición relativa para ciertos grupos. Debido a que el sexo es una característica exógena, se espera que no sea posible rechazar la hipótesis nula de que la posición relativa promedio de hombres es igual a la posición relativa promedio de mujeres, es decir, que se obtenga un p-value mayor a 0.01. Por otro lado, se espera que esto no ocurra para hermanos ni para postulantes en bloque, ya que se produce una propagación del número aleatorio debido a la prioridad de hermano dinámica y a la postulación en bloque.'}<p>
            {df_pvals.to_html()}
        </body>
    </html>
    '''
with open('html_report.html', 'w') as f:
    f.write(html)