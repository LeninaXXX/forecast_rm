-- show all table
SELECT * FROM ANALITICA.dbo.GA_FORECAST_NEW;
SELECT COUNT(*) FROM ANALITICA.dbo.GA_FORECAST_NEW;

-- clean table
TRUNCATE TABLE ANALITICA.dbo.GA_FORECAST_NEW;

-- check last available needed value - appears on top
SELECT * FROM ANALITICA.dbo.GA_MENSUALPARCIAL
	WHERE Origen = 'RED CienRadios'
	ORDER BY FechaFiltro DESC, FechaCreacion DESC;


-- show all table
SELECT * FROM ANALITICA.dbo.GA_FORECAST_NEW;

-- clean table
TRUNCATE TABLE ANALITICA.dbo.GA_FORECAST_NEW;

-- destroy table
DROP TABLE ANALITICA.dbo.GA_FORECAST_NEW;

-- check last available needed value - appears on top
SELECT * FROM ANALITICA.dbo.GA_MENSUALPARCIAL
	WHERE Origen = 'RED CienRadios'
	ORDER BY FechaFiltro DESC, FechaCreacion DESC;

CREATE TABLE ANALITICA.dbo.GA_FORECAST_NEW
(
	UKEY NVARCHAR(100) NOT NULL,
	monthly DECIMAL(22,4),
	Forecast_w_1y DECIMAL(22,4),
	abs_err_w_1y DECIMAL(22,4),
	rel_err_w_1y DECIMAL(22,4),
	per_err_w_1y DECIMAL(22,4),
	Forecast_w_2y DECIMAL(22,4),
	abs_err_w_2y DECIMAL(22,4),
	rel_err_w_2y DECIMAL(22,4),
	per_err_w_2y DECIMAL(22,4),
	Forecast_w_2y_optimal DECIMAL(22,4),
	abs_err_w_2y_optimal DECIMAL(22,4),
	rel_err_w_2y_optimal DECIMAL(22,4),
	per_err_w_2y_optimal DECIMAL(22,4),
	Weight DECIMAL(5,4),		
	FechaFiltro DATE,		
	FechaCreacion DATETIME
);
DROP TABLE ANALITICA.dbo.GA_FORECAST_NEW;
TRUNCATE TABLE ANALITICA.dbo.GA_FORECAST_NEW;
SELECT * FROM ANALITICA.dbo.GA_FORECAST_NEW;
Select UKEY, MONTHLY, Forecast_w_1y, Forecast_w_2y from ga_fORECAST_NEW;

SELECT COUNT(*) FROM ANALITICA.dbo.GA_FORECAST_NEW;
SELECT UKEY, per_err_w_1y, per_err_w_2y, abs(per_err_w_1y - per_err_w_2y) FROM ANALITICA.dbo.GA_FORECAST_NEW;

SELECT * FROM GA_MENSUAL WHERE Origen = 'RED CienRadios';

