
-- 1. Limpa tudo
DROP PROCEDURE IF EXISTS dw.sp_carga_dw;
DROP VIEW IF EXISTS public.vw_preparacao_dw;
DROP TABLE IF EXISTS dw.fato_vendas CASCADE;
DROP TABLE IF EXISTS dw.dim_cliente CASCADE;
DROP TABLE IF EXISTS dw.dim_produto CASCADE;
DROP TABLE IF EXISTS dw.dim_tempo CASCADE;

-- 2. Recria as Tabelas Dimensionais
CREATE TABLE dw.dim_cliente (
    sk_cliente SERIAL PRIMARY KEY,
    cod_usuario_original INT,
    cidade VARCHAR(100),
    estado VARCHAR(50),
    faixa_etaria VARCHAR(50)
);

CREATE TABLE dw.dim_produto (
    sk_produto SERIAL PRIMARY KEY,
    cod_produto_original INT,
    nome_produto VARCHAR(150),
    categoria VARCHAR(100)
);

CREATE TABLE dw.dim_tempo (
    sk_tempo SERIAL PRIMARY KEY,
    data_completa DATE,
    ano INT,
    mes INT,
    trimestre INT
);

CREATE TABLE dw.fato_vendas (
    id_venda SERIAL PRIMARY KEY,
    sk_cliente INT,
    sk_produto INT,
    sk_tempo INT,
    quantidade INT,
    valor_total NUMERIC(10,2),
    forma_pagamento VARCHAR(50),
    CONSTRAINT fk_dim_cliente FOREIGN KEY (sk_cliente) REFERENCES dw.dim_cliente(sk_cliente),
    CONSTRAINT fk_dim_produto FOREIGN KEY (sk_produto) REFERENCES dw.dim_produto(sk_produto),
    CONSTRAINT fk_dim_tempo FOREIGN KEY (sk_tempo) REFERENCES dw.dim_tempo(sk_tempo)
);

-- 3. VIEW  (TRATAMENTO DE DADOS PESADO)
-- AReplace',' por '.' e tiramos as aspas
CREATE OR REPLACE VIEW public.vw_preparacao_dw AS
SELECT 
    -- Limpa aspas do ID
    CAST(REPLACE(v.cod_usuario::TEXT, '"', '') AS INTEGER) AS cod_usuario,
    CAST(REPLACE(v.cod_produto::TEXT, '"', '') AS INTEGER) AS cod_produto,
    
    v.data_compra,
    v.quantidade,
    
    -- TRATAMENTO DE VALOR (Troca vírgula por ponto)
    CAST(
        REPLACE(
            REPLACE(v.valor_compra::TEXT, '"', ''),
            ',', '.'                               
        ) AS NUMERIC
    ) AS valor_compra,
    
    v.forma_pagamento,
    u.cidade,
    u.estado,
    u.faixa_etaria,
    p.nome_produto,
    p.categoria_produto
FROM public.vendas v
JOIN public.usuarios u 
    ON CAST(REPLACE(v.cod_usuario::TEXT, '"', '') AS INTEGER) = CAST(REPLACE(u.cod_usuario::TEXT, '"', '') AS INTEGER)
JOIN public.produtos p 
    ON CAST(REPLACE(v.cod_produto::TEXT, '"', '') AS INTEGER) = CAST(REPLACE(p.cod_produto::TEXT, '"', '') AS INTEGER);


-- 4. PROCEDURE (CARGA)
CREATE OR REPLACE PROCEDURE dw.sp_carga_dw()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Limpa tabelas
    TRUNCATE TABLE dw.fato_vendas CASCADE;
    TRUNCATE TABLE dw.dim_cliente CASCADE;
    TRUNCATE TABLE dw.dim_produto CASCADE;
    TRUNCATE TABLE dw.dim_tempo CASCADE;

    -- Carga Cliente
    INSERT INTO dw.dim_cliente (cod_usuario_original, cidade, estado, faixa_etaria)
    SELECT DISTINCT 
        CAST(REPLACE(cod_usuario::TEXT, '"', '') AS INTEGER), 
        cidade, estado, faixa_etaria 
    FROM public.usuarios;

    -- Carga Produto
    INSERT INTO dw.dim_produto (cod_produto_original, nome_produto, categoria)
    SELECT DISTINCT 
        CAST(REPLACE(cod_produto::TEXT, '"', '') AS INTEGER), 
        nome_produto, categoria_produto 
    FROM public.produtos;

    -- Carga Tempo
    INSERT INTO dw.dim_tempo (data_completa, ano, mes, trimestre)
    SELECT DISTINCT 
        data_compra::DATE, 
        EXTRACT(YEAR FROM data_compra::DATE), 
        EXTRACT(MONTH FROM data_compra::DATE), 
        EXTRACT(QUARTER FROM data_compra::DATE)
    FROM public.vendas;

    -- Carga Fato
    INSERT INTO dw.fato_vendas (sk_cliente, sk_produto, sk_tempo, quantidade, valor_total, forma_pagamento)
    SELECT 
        dc.sk_cliente,
        dp.sk_produto,
        dt.sk_tempo,
        vw.quantidade,
        vw.valor_compra, -- Agora já vem limpo da View como NUMERIC
        vw.forma_pagamento
    FROM public.vw_preparacao_dw vw
    JOIN dw.dim_cliente dc ON vw.cod_usuario = dc.cod_usuario_original
    JOIN dw.dim_produto dp ON vw.cod_produto = dp.cod_produto_original
    JOIN dw.dim_tempo dt ON vw.data_compra::DATE = dt.data_completa;
    
    RAISE NOTICE 'ETL Concluido com sucesso!';
END;
$$;

-- 5. TRIGGER DE AUDITORIA 
CREATE TABLE IF NOT EXISTS public.log_auditoria_vendas (
    id_log SERIAL PRIMARY KEY,
    data_evento TIMESTAMP DEFAULT NOW(),
    mensagem TEXT
);

CREATE OR REPLACE FUNCTION public.fn_log_venda()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO public.log_auditoria_vendas (mensagem)
    VALUES ('Nova venda registrada no sistema OLTP');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tg_nova_venda ON public.vendas;
CREATE TRIGGER tg_nova_venda
AFTER INSERT ON public.vendas
FOR EACH ROW
EXECUTE FUNCTION public.fn_log_venda();



call dw.sp_carga_dw();


select * from dw.fato_vendas limit 10;