-- show all table
SELECT * FROM GA_FORECAST_NEW;

-- clean table
TRUNCATE TABLE GA_FORECAST_NEW;

-- check last available needed value - appears on top
SELECT * FROM GA_MENSUALPARCIAL
	WHERE Origen = 'RED CienRadios'
	ORDER BY FechaFiltro DESC, FechaCreacion DESC;

