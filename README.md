# problem

build a python fastapi to search through a database of linkedin profiles

## project overview

you are building a python fastapi to search through a database of linkedin profiles

you will be using chakra sdk to store the data in a database, and fastapi to build the search api.

you will be using next.js to build the frontend and interact with the fast api.

## run the project

### 1. install the dependencies

```bash
cd python_sheets
pip install -r requirements.txt
```
### 2. create a .env file

```bash
cp .env.example .env
```
add the environment variables

```bash
CHAKRA_DB_SESSION_KEY=your_chakra_db_session_key
NEW_OPENAI_API_KEY=your_new_openai_api_key
MOTHERDUCK_TOKEN=your_motherduck_token
```
### 3. load data to chakra database

create a data folder inside python_sheets and download the linkedin_profiles.parquet file from the google drive link in the slack channel and put it in the data folder

run the following command to load the data to the chakra database

```bash
python python_sheets/chakra_api/chakra_client.py load
```

verify the data is loaded by running the following command

```bash
python python_sheets/chakra_api/chakra_client.py query 10
```

### 4. load data to motherduck

```bash
python python_sheets/loader/loader.py
```

### 5. run the backend

```bash
cd python_sheets
uvicorn main:app --reload
```

### 6. run the frontend

```bash
cd next_streets
npm install
npm run dev
```

## current file structure

```bash
├── /work-trial-sang/
├── python_sheets/
│   ├── main.py
│   ├── api/
│   │   └── endpoints/
│   │       └── profiles.py
│   ├── chakra_api/
│   │   └── chakra_client.py
│   ├── data/
│   │   └── linkedin_profiles.parquet
│   └── models/
│   │    └── profile.py
│   │    └── search.py
│   └── loader/
│        └── loader.py
├── next_streets/
│   ├── app/
│   │   └── page.tsx
│   ├── public/
│   │   └── chakra.jpg
│   ├── .env
│   └── package.json
├── requirements.txt
└── .env
```

## future improvements

- [ ] load and searchmultiple databases
- [ ] embedded vector search
- [ ] remove motherduck
- [ ] handle large datasets
- [ ] graphql to improve embeddings
- [ ] frontend table with links to profiles
