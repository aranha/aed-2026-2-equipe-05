create table if not exists evento_processado (
    evento_id varchar(64) primary key,
    processado_em timestamp with time zone not null default current_timestamp
);

create table if not exists analise_credito (
    solicitacao_id varchar(36) primary key,
    cliente_id varchar(80) not null,
    valor_solicitado decimal(19, 2) not null,
    status varchar(30) not null,
    solicitada_em timestamp with time zone not null
);
