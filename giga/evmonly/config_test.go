package evmonly

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseOCCMode(t *testing.T) {
	mode, err := ParseOCCMode("")
	require.NoError(t, err)
	require.Equal(t, OCCModeBlockSTM, mode)

	mode, err = ParseOCCMode("blockstm")
	require.NoError(t, err)
	require.Equal(t, OCCModeBlockSTM, mode)

	mode, err = ParseOCCMode("snapshot")
	require.NoError(t, err)
	require.Equal(t, OCCModeSnapshot, mode)

	_, err = ParseOCCMode("turbo")
	require.ErrorContains(t, err, "unsupported occ mode")
}

func TestConfigWithDefaultsSelectsBlockSTM(t *testing.T) {
	require.Equal(t, OCCModeBlockSTM, Config{}.WithDefaults().OCCMode)
	require.Equal(t, OCCModeSnapshot, Config{OCCMode: OCCModeSnapshot}.WithDefaults().OCCMode)
}
