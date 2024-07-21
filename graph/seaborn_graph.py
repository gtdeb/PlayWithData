import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import base64
from io import BytesIO

def plot_seaborn_graph(data):
    print(data['graphType'])
    #save_dir="./my-app/graph_images/"
    df = pd.DataFrame(data['plotData'])

    plt.figure(figsize=(10, 6))
    
    if data['graphType'] == 'Scatter':
        sns.scatterplot(data=df)
    elif data['graphType'] == 'Line':
        sns.lineplot(data=df)
    elif data['graphType'] == 'Bar':
        sns.barplot(data=df)
    elif data['graphType'] == 'Histogram':
        sns.histplot(data=df)
    elif data['graphType'] == 'Box':
        sns.boxplot(data=df)
    elif data['graphType'] == 'Violin':
        sns.violinplot(data=df)
    elif data['graphType'] == 'Heatmap':
        sns.heatmap(df.corr())
    elif data['graphType'] == 'Density':
        sns.kdeplot(df)
    elif data['graphType'] == 'Pair':
        sns.pairplot(df)
    elif data['graphType'] == 'Joint':
        sns.jointplot(df)
    elif data['graphType'] == 'FacetGrid':
        sns.FacetGrid(df)
    else:
        raise ValueError(f"Unsupported graph type: {graph_type}")

    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    image_png = buffer.getvalue()
    buffer.close()

    graph_base64 = base64.b64encode(image_png).decode('utf-8')
    return graph_base64
