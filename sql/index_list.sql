-- Table: public.index_list

-- DROP TABLE public.index_list;

CREATE TABLE public.index_list
(
  code character varying NOT NULL,
  name character varying,
  CONSTRAINT pk_il PRIMARY KEY (code)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.index_list
  OWNER TO postgres;
