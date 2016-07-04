-- Table: public.weeklydata

-- DROP TABLE public.weeklydata;

CREATE TABLE public.weeklydata
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
  turnoverrate double precision,
  vol double precision,
  amo double precision,
  totalcap double precision,
  tradeablecap double precision,
  prevclose numeric(6,2),
  hfqratio double precision DEFAULT 1.00,
  CONSTRAINT pk_wd PRIMARY KEY (code, date)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.weeklydata
  OWNER TO postgres;
