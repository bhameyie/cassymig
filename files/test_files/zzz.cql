description: myyoyo
version: 1
up:
  - CREATE TABLE CredentialStorageByEmail(
   email text,
   user_id uuid,
   apitoken text,
   PRIMARY KEY ( email, user_id, apitoken))

down:
  - DROP TABLE CredentialStorageByEmail
