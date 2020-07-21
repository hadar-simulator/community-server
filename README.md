# simple-server
Deploy a server to resolve study on a remote hadar solver

## How to use it
### From server side
start docker image:

`docker run -p 8080:8080 docker.pkg.github.com/hadar-simulator/community-server/server:lastest`

Add: `-e ACCESS_TOKEN=<YOUR_TOKEN>`to apply an basic authentication on your server

### From client side
When you use Hadar locally, you start compute by

`res = solve(study=study)`

Now you have to write

`res = solve(study=study, kind='remote', url='http://my-server', token='your token')`

That all ! Remote your compute with one docker image and one line in your code.
