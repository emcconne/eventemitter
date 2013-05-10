eventemitter
============

TeamForge event handler that publishes Tracker events to RabbitMQ asynchronously.  

Due to the way TeamForge expects packages to be bundled for deployment.  The dependencies of the eventhandler have to be pulled in and unpacked in the deployment jar. 

Users of Maven will need to add the TeamForge sdk and TeamForge sfevents jar files to maven manaully as they aren't availabe from any public Maven repos.

Once installed this eventhandler will connect to RabbitMQ and create a topic exchange called sfevents.  As events occur on Tracker artifacts those events are asynchronously sent to the message bus using a four digit routing key that follows the following pattern:

projectId.trackerId.artifactId.operation.

Examples:

proj1023.*.*.* :  Will receive all tracker events for a project

proj1023.tracker1024.*.* : Will receive all tracker events for a particular tracker

proj1023.*.*.create : Will receive all create events on trackers in a project
