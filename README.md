# File -> Pub/Sub -> Flask (Cloud Run)

- Producer (Cloud Run Job) cita `producer/data.json` i publish-a poruke u Pub/Sub topic.
- Consumer (Cloud Run Service) je Flask app s:
  - `GET /listening`
  - `POST /push` (Pub/Sub push)

Env (producer): `PROJECT_ID` (ili `GOOGLE_CLOUD_PROJECT`) i `TOPIC_ID`.
