So when it created the app the tree was like this:


profilon/databricks-hello-world-app/app.py, requirements.txt, app.yaml

Here is the conentes of requirments.py and app.yaml:
requirments.txt:
streamlit==1.38.0


app.yaml:
command: [
  "streamlit", 
  "run",
  "app.py"
]




So a few things i was thinking of reformating my app to be like this strucutre:

in the root of the project i would have:
.gitignore
LICENSE
NOTICE
README.md
(above files were already included in app and were at rood)
requirments.txt (consiginering moving this to root and not same folder as app.py; is that a problem?)
src/profilon/app.py, app.yaml
src/profilon/pages/1_configure_&_run.py

what are your thoughts on this refactor?
