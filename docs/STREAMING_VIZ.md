# Visualizing Data-in-Motion

First, understand your use case.
What kind of streaming data do you have? How much? How fast?

## Common Choices

- For simple real-time plotting: Use matplotlib.pyplot with FuncAnimation.
- For interactive real-time dashboards: Use Plotly and PyShiny or Dash.
- For high-frequency or large data streams: Use Bokeh and PyShiny or pyqtgraph.

## We will focus on Matplotlib PyPlot with Animation

Other courses cover other options. 
If you've had those courses, you are encouraged to integrate additional features like machine learning, interactive widgets, dashboards, or sentiment analysis to your streaming projects. 

##  Purpose of Matplotlib PyPlot

The pyplot module in Matplotlib provides an interface that works similarly to MATLAB's plotting capabilities. 
It allows us to create, modify, and customize plots with command-style functions like plt.plot(), plt.xlabel(), and plt.show().

## Why Import with Alias plt?

Using the common alias plt is a widely accepted convention in the Python data visualization community. 
This keeps code concise and readable.
Know the source, know how to research the capabilities and how to install matplotlib into your local project virtual environment, and how to import it into Python files that use it. 

## Popular Features (Know These Well)

- Line plots: plt.plot(x, y)
- Bar charts: plt.bar(categories, values)
- Scatter plots: plt.scatter(x, y)
- Histograms: plt.hist(data)
- Labels and Titles: Use plt.xlabel(), plt.ylabel(), and plt.title() to annotate charts.
- Customizing plots using colors, line styles, markers, labels, and legends.
