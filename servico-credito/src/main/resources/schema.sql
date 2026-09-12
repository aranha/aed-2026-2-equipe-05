create table if not exists limite_credito (
    cliente_id varchar(80) primary key,
    limite_total decimal(19, 2) not null,
    limite_disponivel decimal(19, 2) not null,
    atualizado_em timestamp with time zone not null default current_timestamp,
    check (limite_total >= 0),
    check (limite_disponivel >= 0),
    check (limite_disponivel <= limite_total)
);

create table if not exists reserva_limite (
    solicitacao_id varchar(36) primary key,
    evento_origem_id varchar(64) not null unique,
    evento_reserva_id varchar(64) not null unique,
    cliente_id varchar(80) not null,
    valor_reservado decimal(19, 2) not null,
    limite_disponivel_apos decimal(19, 2) not null,
    status varchar(20) not null,
    reservada_em timestamp with time zone not null,
    cancelada_em timestamp with time zone null,
    foreign key (cliente_id) references limite_credito(cliente_id)
);
