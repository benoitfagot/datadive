Welcome to DataDive, a project that we worked on between November 2019 and June 2020, during our last year of Master of Big Data : Intelligent Distributed Systems, in the university of Cergy-Pontoise, France. We had to work remotely during COVID19 but it didn't stop us.<br>
The project was initially called <b>Pattern and trends mining on YELP Data : an application on smart restaurant recommandations</b> but we baptised it as DataDive.

We worked on a remote server since our personal computers didn't have the sufficient memory and CPU to whistand the Data of YELP, which we still had to reduce with 25GB of memory. <br>
The tasks consisted in :
- preprocessing the data : filling missing values with Machine Learning, analysing the disparity, etc
- finding and extracting patterns : we used Data Mining algorithms and statistical analysis to find spatial and temporal trends 
- geolocating users : with the goal of real-time usage, we aimed to apply geo-indistiguishability to retrive user GPS coordinates and recommend him with businesses in his surroudings
- building an hybrid recommandation engine : we used the reviews of known users on known businesses and fed the data to different recommendations algorithms such as Matrix Factorization collaborative filtering, content-based filtering, and more. We also implemented our personal recommandation system for new users that used the results of the pattern mining.
- build an API to request the recommandations : we used Flask framework to build it
- implemente a user interface : since our work was in Python, we decided to give a chance to Django library
<br>
<br>
Disclaimer : unfortunately, most of our cleaned datasets were lost with the closure of the servers and need to be generated again (with a big memory cost) for the recommandation engine to work.
<br>

Members :
<br>
@benoitfagot<br>
@hongphuc95<br>
Mohamed II Bayo<br>
Mina<br>
Ali<br>
