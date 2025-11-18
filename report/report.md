== Transforming bounding box

Because the bounding box is given in RD new and the overture query needs WGS84 coordinates,
I wrote a function to transform the input bounding box using the ```Transformer``` from the pyproj library.