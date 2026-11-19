-- Core banking schema. Applied by the loader inside the target schema (search_path);
-- every statement is idempotent so it can run on every load.

CREATE TABLE IF NOT EXISTS customer (
    customer_id         bigint      PRIMARY KEY,
    customer_type       text        NOT NULL CHECK (customer_type IN ('PERSON', 'ORGANIZATION')),
    first_name          text,
    last_name           text,
    gender              char(1)     CHECK (gender IN ('F', 'M')),
    birth_date          date,
    legal_name          text,
    legal_form          text,
    incorporation_date  date,
    id_document_type    text        CHECK (id_document_type IN ('CIN', 'PASSPORT', 'RC')),
    id_document_number  text,
    tax_id              text,
    nationality         char(2),
    residence_country   char(2)     NOT NULL,
    occupation          text,
    address_line        text        NOT NULL,
    city                text        NOT NULL,
    postal_code         text        NOT NULL,
    phone               text,
    email               text,
    risk_rating         text        NOT NULL CHECK (risk_rating IN ('LOW', 'MEDIUM', 'HIGH')),
    is_pep              boolean     NOT NULL DEFAULT false,
    customer_since      date        NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT person_is_named CHECK (customer_type <> 'PERSON' OR (first_name IS NOT NULL AND last_name IS NOT NULL)),
    CONSTRAINT organization_is_named CHECK (customer_type <> 'ORGANIZATION' OR legal_name IS NOT NULL),
    CONSTRAINT document_is_complete CHECK ((id_document_type IS NULL) = (id_document_number IS NULL))
);

CREATE TABLE IF NOT EXISTS account (
    account_id      bigint      PRIMARY KEY,
    customer_id     bigint      NOT NULL REFERENCES customer (customer_id),
    bank_id         text        NOT NULL,
    account_number  text        NOT NULL,
    rib             char(24)    NOT NULL UNIQUE,
    account_type    text        NOT NULL CHECK (account_type IN ('CURRENT', 'SAVINGS')),
    currency        char(3)     NOT NULL,
    status          text        NOT NULL CHECK (status IN ('ACTIVE', 'DORMANT', 'CLOSED')),
    opened_on       date        NOT NULL,
    closed_on       date,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (bank_id, account_number),
    CONSTRAINT closed_after_opened CHECK (closed_on IS NULL OR closed_on >= opened_on)
);
CREATE INDEX IF NOT EXISTS account_customer_id_idx ON account (customer_id);

-- Cards are referenced by processor token only: the clear PAN never leaves the
-- card management system (PCI DSS scope reduction).
CREATE TABLE IF NOT EXISTS card (
    card_id     bigint      PRIMARY KEY,
    account_id  bigint      NOT NULL REFERENCES account (account_id),
    card_token  char(16)    NOT NULL UNIQUE,
    scheme      text        NOT NULL CHECK (scheme IN ('VISA', 'MASTERCARD', 'CMI')),
    pan_last4   char(4)     NOT NULL,
    product     text        NOT NULL,
    status      text        NOT NULL CHECK (status IN ('ACTIVE', 'BLOCKED', 'EXPIRED')),
    issued_on   date        NOT NULL,
    expires_on  date        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS card_account_id_idx ON card (account_id);

-- Application code does not always maintain updated_at; the database does.
CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER customer_touch_updated_at BEFORE UPDATE ON customer
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE OR REPLACE TRIGGER account_touch_updated_at BEFORE UPDATE ON account
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE OR REPLACE TRIGGER card_touch_updated_at BEFORE UPDATE ON card
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
