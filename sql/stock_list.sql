-- Table: public.stock_list

-- DROP TABLE public.stock_list;

CREATE TABLE public.stock_list
(
  code character varying NOT NULL,
  name character varying,
  status bigint,
  industry character varying,
  CONSTRAINT pk_sl PRIMARY KEY (code)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.stock_list
  OWNER TO postgres;

-- Rule: rule_insert ON public.stock_list

-- DROP RULE rule_insert ON public.stock_list;

CREATE OR REPLACE RULE rule_insert AS
    ON INSERT TO stock_list
   WHERE (EXISTS ( SELECT 1
           FROM stock_list p
          WHERE new.code::text = p.code::text)) DO INSTEAD  UPDATE stock_list SET name = new.name, status = new.status, industry = new.industry
  WHERE stock_list.code::text = new.code::text;

