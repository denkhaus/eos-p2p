package store

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"sync"

	"github.com/denkhaus/eos-p2p/types"
	"github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// BadgerStorer a very simple storer imp for test imp by storer
type BadgerStorer struct {
	chainID          types.Checksum256
	db               *badger.DB
	logger           *zap.Logger
	eg               *errgroup.Group
	mutex            sync.RWMutex
	isStoreAllBlocks bool
	state            *BlockDBState
}

// NewBadgerStorer create a badger storer
func NewBadgerStorer(ctx context.Context, logger *zap.Logger, chainID string, dbPath string, isStoreBlocks bool) (*BadgerStorer, error) {
	cID, err := hex.DecodeString(chainID)
	if err != nil {
		return nil, errors.Wrapf(err, "decode chainID error")
	}

	db, err := badger.Open(badger.DefaultOptions(dbPath))
	if err != nil {
		return nil, errors.Wrapf(err, "create storer %s", dbPath)
	}

	eg, ctx := errgroup.WithContext(ctx)

	res := &BadgerStorer{
		chainID:          cID,
		db:               db,
		logger:           logger,
		isStoreAllBlocks: isStoreBlocks,
		eg:               eg,
		state:            NewBlockDBState(cID),
	}

	if err := res.initState(); err != nil {
		return nil, errors.Wrap(err, "initState")
	}

	return res, nil
}

func (s *BadgerStorer) persistState() error {
	return errors.Wrap(s.db.Update(func(tx *badger.Txn) error {
		bytes, err := s.state.Bytes()
		if err != nil {
			return errors.Wrap(err, "Bytes [state]")
		}
		if err := tx.Set([]byte("state"), bytes); err != nil {
			return errors.Wrap(err, "Set [state]")
		}

		return nil
	}), "init state")
}

func (s *BadgerStorer) initState() error {
	return errors.Wrap(s.db.Update(func(tx *badger.Txn) error {
		state, err := tx.Get([]byte("state"))
		if err != nil && err == badger.ErrKeyNotFound {
			s.logger.Debug("init head state")
			bytes, err := s.state.Bytes()
			if err != nil {
				return errors.Wrap(err, "Bytes [state]")
			}
			if err := tx.Set([]byte("state"), bytes); err != nil {
				return errors.Wrap(err, "Set [state]")
			}

			return nil
		}

		//s.logger.Debug("headstate", zap.String("stat", string(stateBytes)))
		return state.Value(func(stateBytes []byte) error {
			if err := s.state.FromBytes(stateBytes); err != nil {
				return errors.Wrap(err, "FromBytes [state]")
			}

			if !types.IsChecksumEq(s.chainID, s.state.ChainID) {
				return errors.Wrapf(err, "ChainID is diff in store, now: %s, store: %s",
					s.chainID.String(), s.state.ChainID.String())
			}

			return nil
		})

	}), "init state")
}

// ChainID get chainID
func (s *BadgerStorer) ChainID() types.Checksum256 {
	// const
	return s.chainID
}

// HeadBlockNum get headBlockNum
func (s *BadgerStorer) HeadBlockNum() uint32 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.state.HeadBlockNum
}

// State get state data
func (s *BadgerStorer) State() BlockDBState {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return *s.state
}

// HeadBlock get head block
func (s *BadgerStorer) HeadBlock() *types.SignedBlock {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if s.state.HeadBlock == nil {
		return nil
	}

	res, err := types.DeepCopyBlock(s.state.HeadBlock)
	if err != nil {
		s.logger.Error("deep copy error", zap.Error(err))
	}
	return res
}

// HeadBlockID get HeadBlockID
func (s *BadgerStorer) HeadBlockID() types.Checksum256 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.state.HeadBlockID
}

func (s *BadgerStorer) updateStateByBlock(blk *types.SignedBlock) error {
	// Just set to block state
	if s.state.HeadBlockNum >= blk.BlockNumber() {
		return nil
	}

	// s.logger.Info("up block", zap.Uint32("blockNum", blk.BlockNumber()))

	s.state.HeadBlockNum = blk.BlockNumber()
	s.state.HeadBlockID, _ = blk.BlockID()
	s.state.HeadBlockTime = blk.Timestamp.Time
	s.state.HeadBlock, _ = types.DeepCopyBlock(blk)

	if s.state.HeadBlockNum%1000 == 0 {
		s.logger.Info("on block head", zap.Uint32("blockNum", s.state.HeadBlockNum))
	}

	s.state.LastBlocks = append(s.state.LastBlocks, s.state.HeadBlock)
	if len(s.state.LastBlocks) >= maxBlocksHoldInDBStat {
		for i := 0; i < len(s.state.LastBlocks)-1; i++ {
			s.state.LastBlocks[i] = s.state.LastBlocks[i+1]
		}
		s.state.LastBlocks = s.state.LastBlocks[:len(s.state.LastBlocks)-1]
	}

	if s.state.HeadBlockNum%10 == 0 {
		s.eg.Go(func() error {
			s.logger.Debug("persist state")
			if err := s.Flush(); err != nil {
				return errors.Wrap(err, "Flush")
			}
			return nil
		})
	}

	return nil
}

func (s *BadgerStorer) setBlock(blk *types.SignedBlock) error {
	return errors.Wrapf(s.db.Update(func(tx *badger.Txn) error {
		jsonBytes, err := json.Marshal(*blk)
		if err != nil {
			return errors.Wrap(err, "json")
		}

		bID, _ := blk.BlockID()
		if err := tx.Set(bID, jsonBytes); err != nil {
			return errors.Wrap(err, "Set [block]")
		}

		return nil
	}), "set block %d", blk.BlockNumber())
}

// CommitBlock commit block from p2p
func (s *BadgerStorer) CommitBlock(blk *types.SignedBlock) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.updateStateByBlock(blk); err != nil {
		return err
	}

	if s.isStoreAllBlocks {
		return s.setBlock(blk)
	}

	return nil
}

// GetBlockByNum get block by num, if not store all blocks, try to find in state cache
func (s *BadgerStorer) GetBlockByNum(blockNum uint32) (*types.SignedBlock, bool) {
	// TODO: find in blocks
	if !s.isStoreAllBlocks {
		return s.state.getBlockByNum(blockNum)
	}

	return nil, false
}

// CommitTrx commit trx
func (s *BadgerStorer) CommitTrx(_ *types.PackedTransactionMessage) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return nil
}

// Flush flush to db
func (s *BadgerStorer) Flush() error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.persistState()
}

// Close flush storer and close
func (s *BadgerStorer) Close() {
	if err := s.Flush(); err != nil {
		s.logger.Error("close::flush", zap.Error(err))
		return
	}

	if err := s.db.Close(); err != nil {
		s.logger.Error("close::close", zap.Error(err))
	}
}

// Wait on background state persistor
func (s *BadgerStorer) Wait() {
	if err := s.eg.Wait(); err != nil {
		s.logger.Error("wait:wait", zap.Error(err))
	}
}
