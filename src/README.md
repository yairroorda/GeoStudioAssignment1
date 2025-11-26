== About

Student: Yair Roorda\
Sudent number: 5467543\
Course: Geo5019 Geostudio

This repo implements [assignment 1](https://geo5019.notion.site/Assignment-01-Building-Footprints-RESTful-API-28ebfa16e8658012b584d60b11cc706c).
It includes two files, one for setting up the database and one for the API.
The database setup file is `setup_database.py` and the API file is `app.py`.
It also includes a report `report.pdf`that details some of the design and engineering decisions made.

== How to use

To run the code, first setup a venv and install the required packages:
```bash
python -m venv venv
venv\Scripts\activate  # On Windows
pip install -r src\requirements.txt
```

Then, run the database setup script from the src directory to create and populate the database:
```bash
python 01_backend_setup.py
```

Then, start the API server by running:
```bash
uvicorn 02_api:app --reload
```
Everything should now be set up and running. 