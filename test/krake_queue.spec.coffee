fs = require 'fs'
process.env['NODE_ENV'] = 'test'
QueueInterface = require "../krake_queue"
redis = require 'redis'

auth_token = queue_name =  'UNIT_TESTING_QUEUE'
redisInfo =
  "host": "localhost"
  "port": "6379"
  "queueName": queue_name
  "scrapeMode": "depth"

describe "QueueInterface", ->
  beforeEach ()->
    @broadcast_channel = 'UNIT_TESTING_CHANNEL'
    @queue_name = queue_name
    @task_type =  'UNIT_TESTING_TASK_TYPE'
    @auth_token = auth_token
    @task_option_obj = JSON.parse(fs.readFileSync(__dirname + '/fixtures/krake_definition.json').toString())
    @qi = new QueueInterface redisInfo

  afterEach ()->
    @qi.quit()

  it "queue should be running in testing environment", ->
    expect(@qi.environment()).toEqual 'test'

  it "should include UNIT_TESTING in queue_names", ->
    @qi.addTaskToQueue @queue_name, @task_type, @task_option_obj, 'head'
    expect(@queue_name in @qi.queue_names).toBe true

  it "should have an empty queue", (done) ->
    @qi.getNumTaskleft @queue_name, (num) =>
      expect(num).toEqual 0
      done()

  it "should add a task to queue", (done) ->
    @qi.addTaskToQueue @queue_name, @task_type, @task_option_obj, 'head', () =>
      @qi.getNumTaskleft @queue_name, (num) =>
        expect(num).toEqual 1
        done()

  it "should empty queue", (done) ->
    @qi.addTaskToQueue @queue_name, @task_type, @task_option_obj, 'head', () =>
      @qi.getNumTaskleft @queue_name, (num) =>
        @qi.emptyQueue @queue_name, () =>
          @qi.getNumTaskleft @queue_name, (num) =>
            expect(num).toEqual 0
            done()

  it "should successfully broadcast a valid task", (done)->
    @qi.broadcast @auth_token, "new task", "broadcasted message", (status)=>
      expect(status).toEqual true
      done()

  it "should get a task from queue successfully", (done) ->
    @qi.addTaskToQueue @queue_name, @task_type, @task_option_obj, 'head', () =>
      @qi.getTaskFromQueue @queue_name, (task_obj)=>
        expect(typeof task_obj).toBe "object"
        expect(task_obj["task_id"]).toEqual "UNIT_TESTING_QUEUE"
        expect(task_obj["task_type"]).toEqual "UNIT_TESTING_TASK_TYPE"
        done()

  it "should listen for events with channel name equal to auth_token", (done) ->
    message_obj = 
      payload: "what to do?"
    message_string = JSON.stringify message_obj

    @qi.setEventListener "new task", (curr_queue_name, res_obj)=>
      expect(curr_queue_name).toEqual @auth_token
      expect(typeof res_obj).toBe "object"
      expect(res_obj["payload"]).toEqual "what to do?"
      done()

    setTimeout ()=>
      @qi.broadcast @auth_token, "new task", message_obj, (status)=>
        expect(status).toEqual true
    , 100
