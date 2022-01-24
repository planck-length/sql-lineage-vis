# sql-lineage-vis
This  is attempt to fill a need for having a tool that can extract lineage that executed sql code creates. It is far from production tool but here's a demo of how that can possibly be done.

Run it locally
```bash
git clone https://github.com/black-body/sql-lineage-vis.git
cd sql-lineage-vis
python3 -m venv venv
source venv/bin/activate
python -m pip install requirements.txt
export PYTHONPAT=$PYTHONPAT:$(pwd)
export FLASK_APP=home.py
export FLASK_DEBUG=1
cd dev
flask run
```
![home page](https://user-images.githubusercontent.com/53899528/150777408-6fd70631-a442-44d8-92c1-d60f0e46b2e2.png)

![image](https://user-images.githubusercontent.com/53899528/150778392-cd2838fc-ddb2-4168-9be2-9577d6eee97f.png)

![image](https://user-images.githubusercontent.com/53899528/150778552-0350e17b-64db-4577-a2bb-ae35f9c6a17c.png)

![image](https://user-images.githubusercontent.com/53899528/150780123-905f0fbd-11e0-4da6-994c-a677b14a190f.png)
