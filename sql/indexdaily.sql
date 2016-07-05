-- Table: public.indexdaily

-- DROP TABLE public.indexdaily;

CREATE TABLE public.indexdaily
(
  code character varying(10) NOT NULL,
  date date NOT NULL,
  name character varying(30),
  close double precision NOT NULL DEFAULT 0.00,
  high double precision,
  low double precision,
  open double precision,
  netchng double precision,
  pctchng double precision,
  vol double precision,
  amo double precision,
  prevclose double precision,
  CONSTRAINT pk_id PRIMARY KEY (code, date)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.indexdaily
  OWNER TO postgres;

-- Rule: rule_insert_id ON public.indexdaily

-- DROP RULE rule_insert_id ON public.indexdaily;

CREATE OR REPLACE RULE rule_insert_id AS
    ON INSERT TO indexdaily
   WHERE (EXISTS ( SELECT 1
           FROM indexdaily p
          WHERE new.code::text = p.code::text AND new.date = p.date)) DO INSTEAD  UPDATE indexdaily SET close = new.close
  WHERE indexdaily.code::text = new.code::text AND indexdaily.date = new.date;

