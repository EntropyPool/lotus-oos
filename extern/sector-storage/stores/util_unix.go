package stores

import (
	"bytes"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/mitchellh/go-homedir"
	"golang.org/x/xerrors"

	"os"
)

func move(from, to string) error {
	from, err := homedir.Expand(from)
	if err != nil {
		return xerrors.Errorf("move: expanding from: %w", err)
	}

	to, err = homedir.Expand(to)
	if err != nil {
		return xerrors.Errorf("move: expanding to: %w", err)
	}

	if filepath.Base(from) != filepath.Base(to) {
		return xerrors.Errorf("move: base names must match ('%s' != '%s')", filepath.Base(from), filepath.Base(to))
	}

	log.Debugw("move sector data", "from", from, "to", to)

	toDir := filepath.Dir(to)

	// `mv` has decades of experience in moving files quickly; don't pretend we
	//  can do better

	var errOut bytes.Buffer
	cmd := exec.Command("/usr/bin/env", "mv", "-v", "-t", toDir, from) // nolint
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return xerrors.Errorf("exec mv (stderr: %s): %w", strings.TrimSpace(errOut.String()), err)
	}

	return nil
}

func upload(from string, prefix string, objName string, cli *OSSClient) error {
	stat, err := os.Stat(from)
	if err != nil {
		return err
	}

	if stat.IsDir() {
		ents, err := ioutil.ReadDir(from)
		if err != nil {
			return err
		}
		for _, ent := range ents {
			filePath := filepath.Join(from, ent.Name())
			entObjName := filepath.Join(objName, ent.Name())
			err = cli.UploadObject(prefix, entObjName, filePath)
			if err != nil {
				return err
			}
		}
	} else {
		err = cli.UploadObject(prefix, objName, from)
		if err != nil {
			return err
		}
	}

	return nil
}
