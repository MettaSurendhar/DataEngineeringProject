#!/bin/bash

echo "Initializing Data Pipeline...."
python ./dataExtraction.py
echo  "Completed Extraction !"
python ./dataTransformation.py
echo  "Completed Transformation !"
python ./dataLoad.py
echo  "Data Loaded Successfully!"
python ./app.py
echo   "Server Started! Go to http://127.0.0.1:5000/"