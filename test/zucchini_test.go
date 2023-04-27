package test

import (
	"testing"
	"time"

	"github.com/addisoncox/zucchini"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func Test(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Zucchini Test")
}

var _ = Describe("Test Zucchini", func() {
	redis := zucchini.NewRedisClient("localhost:6379", "", 0)
	var taskProducer zucchini.Producer[Numbers, int]
	var taskConsumer zucchini.Consumer[Numbers, int]

	addr := "localhost:9080"

	Describe("Testing Task States", Ordered, func() {

		Context("Can See Failed Task", func() {
			BeforeEach(func() {
				redis.FlushAll()
				taskProducer = zucchini.NewProducer(
					AlwaysFailsTaskDefinition,
					redis,
				)

				taskConsumer = zucchini.NewConsumer(
					AlwaysFailsTaskDefinition,
					redis,
					12,
				)
				go taskConsumer.StartMonitorServer(addr)
				go taskConsumer.ProcessTasks()
			})
			It("Should Be Failed", func(ctx SpecContext) {
				taskID := taskProducer.QueueTask(Numbers{X: 0, Y: 1})
				time.Sleep(time.Second * 2)
				Expect(GetStatusFromMonitor(addr, taskID)).To(Equal("failed"))
			}, SpecTimeout(time.Second*5))
		})

		Context("Can See Successfully Executed Task", func() {
			BeforeEach(func() {
				redis.FlushAll()
				taskProducer = zucchini.NewProducer(
					AlwaysSucceedsTaskDefinition,
					redis,
				)

				taskConsumer = zucchini.NewConsumer(
					AlwaysSucceedsTaskDefinition,
					redis,
					10,
				)
				go taskConsumer.StartMonitorServer(addr)
				go taskConsumer.ProcessTasks()
			})

			It("Should Succeeded", func(ctx SpecContext) {
				taskID := taskProducer.QueueTask(Numbers{})
				time.Sleep(time.Second * 2)
				Expect(GetStatusFromMonitor(addr, taskID)).To(Equal("succeeded"))
			}, SpecTimeout(time.Second*5))
		})

		Context("Can See Cancelled Task", func() {
			It("Should Be Cancelled", func(ctx SpecContext) {
				taskID := taskProducer.QueueTask(Numbers{})
				taskProducer.CancelTask(taskID)
				time.Sleep(time.Second * 2)
				Expect(GetStatusFromMonitor(addr, taskID)).To(Equal("cancelled"))
			}, SpecTimeout(time.Second*5))
		})

		Context("Can See Queued Task", func() {
			BeforeEach(func() {
				redis.FlushAll()
				taskProducer = zucchini.NewProducer(
					SleepAddTaskDefinition,
					redis,
				)

				taskConsumer = zucchini.NewConsumer(
					SleepAddTaskDefinition,
					redis,
					10,
				)
				go taskConsumer.StartMonitorServer(addr)
				go taskConsumer.ProcessTasks()
			})
			It("Should Be Queued", func(ctx SpecContext) {
				for i := 0; i < 10; i++ {
					taskProducer.QueueTask(Numbers{X: 0, Y: 1})
				}
				taskID := taskProducer.QueueTask(Numbers{X: 0, Y: 1})
				time.Sleep(time.Second * 2)
				Expect(GetStatusFromMonitor(addr, taskID)).To(Equal("queued"))
			}, SpecTimeout(time.Second*5))
		})

		Context("Exponential Backoff Works", func() {
			BeforeEach(func() {
				redis.FlushAll()

				taskProducer = zucchini.NewProducer(
					AlwaysFailsWithRetiresTaskDefinition,
					redis,
				)

				taskConsumer = zucchini.NewConsumer(
					AlwaysFailsWithRetiresTaskDefinition,
					redis,
					10,
				)
				go taskConsumer.StartMonitorServer(addr)
				go taskConsumer.ProcessTasks()

			})
			It("Should have exponential backoff on second retry", func(ctx SpecContext) {
				taskID := taskProducer.QueueTask(Numbers{X: 0, Y: 1})
				time.Sleep(time.Second)
				Expect(GetRetriesFromMonitor(addr, taskID)).To(Equal(1.0))
				time.Sleep(time.Second)
				Expect(GetRetriesFromMonitor(addr, taskID)).To(Equal(1.0))
				time.Sleep(time.Second)
				Expect(GetRetriesFromMonitor(addr, taskID)).To(Equal(2.0))
			}, SpecTimeout(time.Second*5))
		})
	})
})
