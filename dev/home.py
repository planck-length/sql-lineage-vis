from flask import Flask, request
from flask import render_template
import os
app = Flask(__name__)


@app.route('/sql_editor_frame')
def get_sql_editor_frame():
    return render_template('sql_editor_frame.html')


@app.route('/graph_frame', methods=['GET','POST'])
def get_graph_frame():
    if request.method == "POST":
        r_t = render_template('graph_frame.html', input=str(request.form))
        with open("templates/graph_frame_templated.html",'w') as tw:
            tw.write(r_t)
        return render_template('sql_editor_frame.html')
    if os.path.exists("templates/graph_frame_templated.html"):
        return render_template('graph_frame_templated.html')
    return render_template('graph_frame.html')


@app.route('/')
def home():
    return render_template('home.html')


if __name__ == "__main__":
    app.run(debug=True)
