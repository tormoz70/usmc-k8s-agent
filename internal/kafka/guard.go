package kafka

import (
	"fmt"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/policy"
)

// CommandGuard validates Kafka command envelope and headers against policy.
type CommandGuard struct {
	policy *policy.Engine
}

func NewCommandGuard(engine *policy.Engine) *CommandGuard {
	return &CommandGuard{policy: engine}
}

func (g *CommandGuard) Validate(cmd *command.Command, meta command.RequestMeta) error {
	if g == nil || g.policy == nil {
		return fmt.Errorf("command guard is not configured")
	}
	if err := cmd.Validate(); err != nil {
		return err
	}
	if err := g.policy.AllowCommandType(cmd.Type); err != nil {
		return err
	}
	if err := g.policy.AllowIssuer(cmd.Issuer); err != nil {
		return err
	}
	if err := g.policy.AllowReplyTopic(meta.ReplyTopic); err != nil {
		return err
	}
	return nil
}

// CanPublishReply reports whether a response may be sent to the reply topic.
func (g *CommandGuard) CanPublishReply(topic string) bool {
	if g == nil || g.policy == nil {
		return false
	}
	return g.policy.AllowReplyTopic(topic) == nil
}
