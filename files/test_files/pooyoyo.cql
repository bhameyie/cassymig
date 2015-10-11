description: pooyoyo
version: 3
up:
  - CREATE TABLE CredentialStorageByEmail(
   email text,
   user_id uuid,
   apitoken text,
   PRIMARY KEY ( email, user_id, apitoken))
  - POOF

down:
  - DROP TABLE CredentialStorageByEmail
  - BLA BLA BLA
