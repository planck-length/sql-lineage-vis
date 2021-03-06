from flask import Flask, request, Response
from flask import render_template
import os
import sys
from datetime import datetime
from app.sql_lineage_vis import SqlLineageVis
app = Flask(__name__)


@app.route('/sql_editor_frame')
def get_sql_editor_frame():
    return render_template('sql_editor_frame.html')

@app.route('/graph_image',methods=['GET'])
def get_graph_image():
    return render_template('graph_frame.html')

@app.route('/graph_frame', methods=['POST'])
def get_graph_frame():
    app.logger.debug(request.method)
    if request.method == "POST":
        app.logger.debug("CREATING LINEAGE")
        app.logger.debug(request.data)
        sql_lin_vis=SqlLineageVis()
        try:
            sql_lin_vis.vis_sql_lineage(str(request.data,'utf-8'))
        except Exception as ex:
            return Response(str(ex),status=500)
        return Response(status=201)
    raise Response(status=400)


@app.route('/')
def home():
    return render_template('home.html')


if __name__ == "__main__":
    app.run(debug=True)
