package version

import (
	"github.com/elixirhealth/service-base/pkg/version"
)

// Current contains the current build info.
var Current version.BuildInfo

// these variables are populated by ldflags during builds and fall back to population from git repo
// when they're not set (e.g., during tests)
var (
	// GitBranch is the current git branch
	GitBranch string

	// GitRevision is the current git commit hash.
	GitRevision string

	// BuildDate is the date of the build.
	BuildDate string
)

const currentSemverString = "0.1.0"

func init() {
	Current = version.GetBuildInfo(GitBranch, GitRevision, BuildDate, currentSemverString)
}
