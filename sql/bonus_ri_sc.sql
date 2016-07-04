-- Table: public.bonus_ri_sc

-- DROP TABLE public.bonus_ri_sc;

CREATE TABLE public.bonus_ri_sc
(
  code character varying(10) NOT NULL,
  adate date NOT NULL,
  give real NOT NULL DEFAULT 0,
  transfer real NOT NULL DEFAULT 0,
  paydiv real NOT NULL DEFAULT 0,
  ri real NOT NULL DEFAULT 0,
  riprice real DEFAULT 0,
  basecap real DEFAULT 0,
  rdate date,
  xdate date NOT NULL,
  reason character varying(30),
  totalshare double precision DEFAULT 0.0,
  tradeshare double precision NOT NULL DEFAULT 0.0,
  limitshare double precision DEFAULT 0.0,
  type character varying(20) NOT NULL,
  prevts double precision DEFAULT 0.0,
  CONSTRAINT pk_bsr PRIMARY KEY (code, adate, xdate, paydiv, ri, tradeshare)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.bonus_ri_sc
  OWNER TO postgres;
