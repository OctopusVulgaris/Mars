-- Table: public.dailydata

-- DROP TABLE public.dailydata;

CREATE TABLE public.dailydata
(
  code character varying(10) NOT NULL,
  date date NOT NULL,
  name character varying(30),
  close numeric(6,2) NOT NULL DEFAULT 0.00,
  high numeric(6,2),
  low numeric(6,2),
  open numeric(6,2),
  netchng numeric(6,2),
  pctchng double precision,
  turnoverrate double precision DEFAULT 0,
  vol double precision DEFAULT 0,
  amo double precision DEFAULT 0,
  totalcap double precision DEFAULT 0,
  tradeablecap double precision DEFAULT 0,
  prevclose numeric(6,2),
  hfqratio double precision NOT NULL DEFAULT 1.00,
  CONSTRAINT pk_dd PRIMARY KEY (code, date)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.dailydata
  OWNER TO postgres;

-- Rule: rule_insert ON public.dailydata

-- DROP RULE rule_insert ON public.dailydata;

CREATE OR REPLACE RULE rule_insert AS
    ON INSERT TO dailydata
   WHERE (EXISTS ( SELECT 1
           FROM dailydata p
          WHERE new.code::text = p.code::text AND new.date = p.date)) DO INSTEAD  UPDATE dailydata SET hfqratio = new.hfqratio, name = new.name, close = new.close, high = new.high, low = new.low, open = new.open, netchng = new.netchng, pctchng = new.pctchng, turnoverrate = new.turnoverrate, vol = new.vol, amo = new.amo, totalcap = new.totalcap, tradeablecap = new.tradeablecap, prevclose = new.prevclose
  WHERE dailydata.code::text = new.code::text AND dailydata.date = new.date;

