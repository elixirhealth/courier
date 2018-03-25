package cmd

import (
	"html/template"
	"io"
	"runtime"
	"time"

	cerrors "github.com/drausin/libri/libri/common/errors"
	"github.com/elixirhealth/courier/version"
)

const bannerTemplate = `

Courier Server

Version   	{{ .Version }}
Build Date:     {{ .BuildDate }}
Branch:      	{{ .GitBranch }}
Revision:   	{{ .GitRevision }}
Go version:     {{ .GoVersion }}
GOOS:           {{ .GoOS }}
GOARCH:         {{ .GoArch }}
NumCPU:         {{ .NumCPU }}

`

type bannerConfig struct {
	Version     string
	GitBranch   string
	GitRevision string
	BuildDate   string
	Now         string
	GoVersion   string
	GoOS        string
	GoArch      string
	NumCPU      int
}

// writeBanner writes the librarian banner to the io.Writer.
func writeBanner(w io.Writer) {
	config := &bannerConfig{
		Version:     version.Current.Version.String(),
		GitBranch:   version.Current.GitBranch,
		GitRevision: version.Current.GitRevision,
		BuildDate:   version.Current.BuildDate,
		Now:         time.Now().UTC().Format(time.RFC3339),
		GoVersion:   runtime.Version(),
		GoOS:        runtime.GOOS,
		GoArch:      runtime.GOARCH,
		NumCPU:      runtime.NumCPU(),
	}
	tmpl, err := template.New("librarian-banner").Parse(bannerTemplate)
	cerrors.MaybePanic(err)
	err = tmpl.Execute(w, config)
	cerrors.MaybePanic(err)
}
