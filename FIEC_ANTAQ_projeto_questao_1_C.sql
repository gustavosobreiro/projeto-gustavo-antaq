CREATE TABLE [dbo].[atracacao_fato](
	[IDAtracacao] [int] NOT NULL,
	[CDTUP] [varchar](120) NULL,
	[IDBerco] [varchar](120) NULL,
	[Berco] [varchar](120) NULL,
	[Porto_Atracacao] [varchar](120) NULL,
	[Apelido_Instalacao_Portuaria] [varchar](120) NULL,
	[Complexo_Portuario] [varchar](120) NULL,
	[Tipo_da_Autoridade_Portuaria] [varchar](120) NULL,
	[Data_Atracacao] [varchar](120) NULL,
	[Data_Chegada] [varchar](120) NULL,
	[Data_Desatracacao] [varchar](120) NULL,
	[Data_Inicio_Operacao] [varchar](120) NULL,
	[Data_Termino_Operacao] [varchar](120) NULL,
	[Ano] [int] NULL,
	[Mes] [varchar](120) NULL,
	[Tipo_de_Operacao] [varchar](120) NULL,
	[Tipo_de_Navegacao_da_Atracacao] [varchar](120) NULL,
	[Nacionalidade_do_Armador] [varchar](120) NULL,
	[FlagMCOperacaoAtracacao] [varchar](120) NULL,
	[Terminal] [varchar](120) NULL,
	[Municipio] [varchar](120) NULL,
	[UF] [varchar](120) NULL,
	[SGUF] [varchar](120) NULL,
	[Regiao_Geografica] [varchar](120) NULL,
	[N_da_Capitania] [varchar](120) NULL,
	[N_do_IMO] [varchar](120) NULL,
	[TEsperaAtracacao] [real] NULL,
	[TEsperaInicioOp] [real] NULL,
	[TOperacao] [real] NULL,
	[TEsperaDesatracacao] [real] NULL,
	[TAtracado] [real] NULL,
	[TEstadia] [real] NULL

    
SELECT
CASE WHEN Ano IN ('2020','2021') THEN 'Brasil'
ELSE 'Exterior'
END AS 'Localidade',
COUNT (DISTINCT IDAtracacao) AS 'Numero_de_atracacoes', AVG (TEsperaAtracacao) AS 'Tempo_de_espera_medio',
AVG (TAtracado) AS 'Tempo_atracado_medio', Mes, Ano
FROM atracacao_fato WHERE Ano IN ('2020','2021')
GROUP BY Ano, Mes

SELECT Regiao_Geografica, COUNT (DISTINCT IDAtracacao) AS 'Numero_de_atracacoes', AVG (TEsperaAtracacao) AS 'Tempo_de_espera_medio',
AVG (TAtracado) AS 'Tempo_atracado_medio', Mes, Ano
FROM atracacao_fato WHERE Regiao_Geografica LIKE 'Nordeste' AND Ano IN ('2020','2021')
GROUP BY Ano, Mes, Regiao_Geografica

SELECT UF, COUNT (DISTINCT IDAtracacao) AS 'Numero_de_atracacoes', AVG (TEsperaAtracacao) AS 'Tempo_de_espera_medio',
AVG (TAtracado) AS 'Tempo_atracado_medio', Mes, Ano
FROM atracacao_fato WHERE UF LIKE 'Ceará' AND Ano IN ('2020','2021')
GROUP BY Ano, Mes, UF