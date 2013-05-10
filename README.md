eventemitter
============

TeamForge event handler that publishes Tracker events to RabbitMQ asynchronously.  

Due to the way TeamForge expects packages to be bundled for deployment.  The dependencies of the eventhandler have to be pulled in and unpacked in the deployment jar. 

Users of Maven will need to add the TeamForge sdk and TeamForge sfevents jar files to maven manaully as they aren't availabe from any public Maven repos.

