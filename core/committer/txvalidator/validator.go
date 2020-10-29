/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package txvalidator

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	commonerrors "github.com/hyperledger/fabric/common/errors"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/chaincode/platforms"
	"github.com/hyperledger/fabric/core/chaincode/platforms/golang"
	"github.com/hyperledger/fabric/core/common/sysccprovider"
	"github.com/hyperledger/fabric/core/common/validation"
	"github.com/hyperledger/fabric/core/ledger"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protos/common"
	mspprotos "github.com/hyperledger/fabric/protos/msp"
	ab "github.com/hyperledger/fabric/protos/orderer"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/pkg/errors"
)

// Support provides all of the needed to evaluate the VSCC
type Support interface {
	// Acquire implements semaphore-like acquire semantics
	Acquire(ctx context.Context, n int64) error

	// Release implements semaphore-like release semantics
	Release(n int64)

	// Ledger returns the ledger associated with this validator
	Ledger() ledger.PeerLedger

	// MSPManager returns the MSP manager for this channel
	MSPManager() msp.MSPManager

	// Apply attempts to apply a configtx to become the new config
	Apply(configtx *common.ConfigEnvelope) error

	// GetMSPIDs returns the IDs for the application MSPs
	// that have been defined in the channel
	GetMSPIDs(cid string) []string

	// Capabilities defines the capabilities for the application portion of this channel
	Capabilities() channelconfig.ApplicationCapabilities
}

//Validator interface which defines API to validate block transactions
// and return the bit array mask indicating invalid transactions which
// didn't pass validation.
type Validator interface {
	Validate(block *common.Block) error
}

// private interface to decouple tx validator
// and vscc execution, in order to increase
// testability of TxValidator
type vsccValidator interface {
	VSCCValidateTx(seq int, payload *common.Payload, envBytes []byte, block *common.Block) (error, peer.TxValidationCode)
}

// implementation of Validator interface, keeps
// reference to the ledger to enable tx simulation
// and execution of vscc
type TxValidator struct {
	ChainID string
	Support Support
	Vscc    vsccValidator
	// !!! BEGIN MODIFICATION
	KafkaOffset         int
	NewKafkaOffset      int
	ConnectOrTTCOffsets []int
}

type kafkaValidationResult struct {
	kafkaOffset int
	err         error
}

// !!! END MODIFICATION

var logger = flogging.MustGetLogger("committer.txvalidator")

type blockValidationRequest struct {
	block *common.Block
	d     []byte
	tIdx  int
}

type blockValidationResult struct {
	tIdx                 int
	validationCode       peer.TxValidationCode
	txsChaincodeName     *sysccprovider.ChaincodeInstance
	txsUpgradedChaincode *sysccprovider.ChaincodeInstance
	err                  error
	txid                 string
}

// NewTxValidator creates new transactions validator
func NewTxValidator(chainID string, support Support, sccp sysccprovider.SystemChaincodeProvider, pm PluginMapper) *TxValidator {
	// Encapsulates interface implementation
	pluginValidator := NewPluginValidator(pm, support.Ledger(), &dynamicDeserializer{support: support}, &dynamicCapabilities{support: support})
	return &TxValidator{
		ChainID:     chainID,
		Support:     support,
		Vscc:        newVSCCValidator(chainID, support, sccp, pluginValidator),
		KafkaOffset: 0}
}

func (v *TxValidator) chainExists(chain string) bool {
	// TODO: implement this function!
	return true
}

// Validate performs the validation of a block. The validation
// of each transaction in the block is performed in parallel.
// The approach is as follows: the committer thread starts the
// tx validation function in a goroutine (using a semaphore to cap
// the number of concurrent validating goroutines). The committer
// thread then reads results of validation (in orderer of completion
// of the goroutines) from the results channel. The goroutines
// perform the validation of the txs in the block and enqueue the
// validation result in the results channel. A few note-worthy facts:
// 1) to keep the approach simple, the committer thread enqueues
//    all transactions in the block and then moves on to reading the
//    results.
// 2) for parallel validation to work, it is important that the
//    validation function does not change the state of the system.
//    Otherwise the order in which validation is perform matters
//    and we have to resort to sequential validation (or some locking).
//    This is currently true, because the only function that affects
//    state is when a config transaction is received, but they are
//    guaranteed to be alone in the block. If/when this assumption
//    is violated, this code must be changed.
func (v *TxValidator) Validate(block *common.Block) error {
	var err error
	var errPos int

	startValidation := time.Now() // timer to log Validate block duration
	logger.Debugf("[%s] START Block Validation for block [%d]", v.ChainID, block.Header.Number)

	// !!! BEGIN MODIFICATION
	v.NewKafkaOffset = v.KafkaOffset

	//Here we regain information about how and why the block was cut by the orderer
	//First we retrieve the KafkaMeta data containing either the TTC Messages / Connect Messages or information about the block size
	ordererMetadata := &common.Metadata{}
	err = proto.Unmarshal(block.Metadata.Metadata[common.BlockMetadataIndex_ORDERER], ordererMetadata)
	if err != nil {
		return err
	}

	kafkaMetadata := &ab.KafkaMetadata{}
	err = proto.Unmarshal(ordererMetadata.Value, kafkaMetadata)
	if err != nil {
		return err
	}

	//Afterwards we check, if the orderer was correct to cut the block here
	if err = v.validateConnectOrTTCMessages(kafkaMetadata); err != nil {
		return err
	}

	// !!! END MODIFICATION

	// Initialize trans as valid here, then set invalidation reason code upon invalidation below
	txsfltr := ledgerUtil.NewTxValidationFlags(len(block.Data.Data))
	// txsChaincodeNames records all the invoked chaincodes by tx in a block
	txsChaincodeNames := make(map[int]*sysccprovider.ChaincodeInstance)
	// upgradedChaincodes records all the chaincodes that are upgraded in a block
	txsUpgradedChaincodes := make(map[int]*sysccprovider.ChaincodeInstance)
	// array of txids
	txidArray := make([]string, len(block.Data.Data))

	results := make(chan *blockValidationResult)

	// !!! BEGIN MODIFICATION
	additionalKafkaOffset := 0
	go func() {
		for tIdx, d := range block.Data.Data {
			// ensure that we don't have too many concurrent validation workers
			v.Support.Acquire(context.Background(), 1)

			for v.ConnectOrTTCOffsets != nil && v.NewKafkaOffset+tIdx+additionalKafkaOffset == v.ConnectOrTTCOffsets[0] {
				logger.Infof("!!§!! Corrected Offset %d to %d !!§!!", v.NewKafkaOffset+tIdx+additionalKafkaOffset, v.NewKafkaOffset+tIdx+additionalKafkaOffset+1)
				logger.Infof("!!§!! Offsets remaining in Queue: ", v.ConnectOrTTCOffsets, " !!§!!")
				additionalKafkaOffset++
				if len(v.ConnectOrTTCOffsets) == 1 {
					v.ConnectOrTTCOffsets = nil
				} else {
					v.ConnectOrTTCOffsets = v.ConnectOrTTCOffsets[1:]
				}
			}

			go func(index int, additionalOffset int, data []byte) {
				defer v.Support.Release(1)

				v.validateTx(&blockValidationRequest{
					d:     data,
					block: block,
					tIdx:  index,
				}, additionalOffset, results)
			}(tIdx, additionalKafkaOffset, d)
		}
	}()
	// !!! END MODIFICATION

	logger.Debugf("expecting %d block validation responses", len(block.Data.Data))

	// now we read responses in the order in which they come back
	for i := 0; i < len(block.Data.Data); i++ {
		res := <-results

		if res.err != nil {
			// if there is an error, we buffer its value, wait for
			// all workers to complete validation and then return
			// the error from the first tx in this block that returned an error
			logger.Debugf("got terminal error %s for idx %d", res.err, res.tIdx)

			if err == nil || res.tIdx < errPos {
				err = res.err
				errPos = res.tIdx
			}
		} else {
			// if there was no error, we set the txsfltr and we set the
			// txsChaincodeNames and txsUpgradedChaincodes maps
			logger.Debugf("got result for idx %d, code %d", res.tIdx, res.validationCode)

			txsfltr.SetFlag(res.tIdx, res.validationCode)

			if res.validationCode == peer.TxValidationCode_VALID {
				if res.txsChaincodeName != nil {
					txsChaincodeNames[res.tIdx] = res.txsChaincodeName
				}
				if res.txsUpgradedChaincode != nil {
					txsUpgradedChaincodes[res.tIdx] = res.txsUpgradedChaincode
				}
				txidArray[res.tIdx] = res.txid
			}
		}
	}

	// if we're here, all workers have completed the validation.
	// If there was an error we return the error from the first
	// tx in this block that returned an error
	if err != nil {
		return err
	}

	// !!! BEGIN MODIFICATION

	v.NewKafkaOffset += len(block.Data.Data) + additionalKafkaOffset

	for len(v.ConnectOrTTCOffsets) > 0 {
		if v.NewKafkaOffset != v.ConnectOrTTCOffsets[0] {
			logger.Errorf("!!§!! Unhandled Connect/TTC Message contains invalid Sequence Number !!§!!")
			return fmt.Errorf("!!§!! Unhandled Connect/TTC Message contains invalid Sequence Number !!§!! ")
		}
		v.NewKafkaOffset++
		v.ConnectOrTTCOffsets = v.ConnectOrTTCOffsets[1:]
	}

	v.ConnectOrTTCOffsets = nil

	//Afterwards we check, if the orderer was correct to cut the block here
	if err = v.validateTTCMessages(kafkaMetadata); err != nil {
		return err
	}

	logger.Infof("!!§!! Kafka Offset updated old ", v.KafkaOffset, " new ", v.NewKafkaOffset)
	v.KafkaOffset = v.NewKafkaOffset
	// !!! END MODIFICATION

	// if we operate with this capability, we mark invalid any transaction that has a txid
	// which is equal to that of a previous tx in this block
	if v.Support.Capabilities().ForbidDuplicateTXIdInBlock() {
		markTXIdDuplicates(txidArray, txsfltr)
	}

	// if we're here, all workers have completed validation and
	// no error was reported; we set the tx filter and return
	// success
	v.invalidTXsForUpgradeCC(txsChaincodeNames, txsUpgradedChaincodes, txsfltr)

	// make sure no transaction has skipped validation
	err = v.allValidated(txsfltr, block)
	if err != nil {
		return err
	}

	// Initialize metadata structure
	utils.InitBlockMetadata(block)

	block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER] = txsfltr

	elapsedValidation := time.Since(startValidation) / time.Millisecond // duration in ms
	logger.Infof("[%s] Validated block [%d] in %dms", v.ChainID, block.Header.Number, elapsedValidation)

	return nil
}

//Here, the signature and merkle proof of the kafka cluster is validated.
func (v *TxValidator) validateConnectOrTTCMessages(kafkaMetadata *ab.KafkaMetadata) error {
	if len(kafkaMetadata.ConnectOrTTCPayload) != 0 {
		logger.Infof("!!§!! Got Connect of TTC Messages of old blocks !!§!!")

		results := make(chan *kafkaValidationResult)
		go func() {
			for _, payload := range kafkaMetadata.ConnectOrTTCPayload {
				go func(data *ab.KafkaPayload) {
					v.validateKafkaPayload(data, results)
				}(payload)
			}
		}()

		// handles returned offsets
		v.ConnectOrTTCOffsets = make([]int, len(kafkaMetadata.ConnectOrTTCPayload))
		var err error
		for i := 0; i < len(kafkaMetadata.ConnectOrTTCPayload); i++ {
			res := <-results
			// if we get an error we buffer it and wait till all goroutines have terminated
			// it is sufficient to return a single error in case there is one
			if res.err != nil {
				err = res.err
			}
			v.ConnectOrTTCOffsets[i] = res.kafkaOffset
		}
		if err != nil {
			return err
		}

		// sorts slice in increasing order
		sort.Ints(v.ConnectOrTTCOffsets)
		logger.Infof("!!§!! Offsets in Queue after Sorting: ", v.ConnectOrTTCOffsets, " !!§!!")
		// the gathered offsets can contain gaps (e.g if the orderer enqueued another envelope before sending the TTC)
		// thus, we only update the v.kafkaOffset value as far as we can and fill the gaps later on
		// invariant: v.ConnectOrTTCOffsets has to be empty after the block is fully validated
		for offset := range v.ConnectOrTTCOffsets {
			if offset == v.NewKafkaOffset {
				v.NewKafkaOffset++
			} else {
				break
			}
		}
		//update list to the offset after the first gap
		if v.NewKafkaOffset-v.KafkaOffset == len(v.ConnectOrTTCOffsets) {
			v.ConnectOrTTCOffsets = nil
		} else {
			v.ConnectOrTTCOffsets = v.ConnectOrTTCOffsets[v.NewKafkaOffset-v.KafkaOffset:]
		}
		logger.Infof("!!§!! Offsets remaining in Queue: ", v.ConnectOrTTCOffsets, " !!§!!")
	}
	return nil
}

//Here, the signature and merkle proof of the kafka cluster is validated.
func (v *TxValidator) validateTTCMessages(kafkaMetadata *ab.KafkaMetadata) error {
	if kafkaMetadata.TTCPayload != nil {
		logger.Infof("!!§!! Got TTC Message of current block !!§!!")
		// validate
		results := make(chan *kafkaValidationResult)
		go func() {
			v.validateKafkaPayload(kafkaMetadata.TTCPayload, results)
		}()

		// check response for kafkaOffset and errors
		response := <-results
		if response.err != nil {
			return response.err
		}

		// all gaps of offsets should be closed and the offset of the ttc message should be the last one
		if v.NewKafkaOffset != response.kafkaOffset {
			logger.Errorf("!!§!! Invalid Sequence Number of TTC Message !!§!!")
			return fmt.Errorf("!!§!! Invalid Sequence Number of TTC Message !!§!! ")
		}

		// otherwise update
		logger.Infof("!!§!! Kafka Offset updated (TTC Message)")
		v.NewKafkaOffset++
	}
	return nil
}

func (v *TxValidator) validateKafkaPayload(payload *ab.KafkaPayload, responseChannel chan<- *kafkaValidationResult) {
	logger.Infof("!!§!! Retrieving Sequence Number !!§!!")
	offset := int(binary.BigEndian.Uint64(payload.ConsumerMessageBytes[0:8]))

	proof := GetProofFromBytes(payload.KafkaMerkleProofHeader)

	logger.Infof("!!§!! Checking TTC/Connect Kafka Proof !!§!!")

	//Verify Merkle Proof
	if !proof.VerifyProof(payload.ConsumerMessageBytes) {
		logger.Errorf("!!$!! Invalid Merkle Proof for TTC/Connect Message !!$!! ")
		responseChannel <- &kafkaValidationResult{
			kafkaOffset: offset,
			err:         fmt.Errorf("!!$!! Invalid Merkle Proof for TTC/Connect Message !!$!! "),
		}
	}

	logger.Infof("!!§!! TTC/Connect Kafka Proof is valid !!§!!")

	logger.Infof("!!§!! Checking Kafka Signature of TTC/Connect Message !!§!!")

	//Verify Signature
	if proof.VerifySignature(payload.KafkaSignatureHeader) != nil {
		logger.Errorf("!!$!! Invalid Kafka Signature for TTC/Connect Message !!$!!")
		responseChannel <- &kafkaValidationResult{
			kafkaOffset: offset,
			err:         fmt.Errorf("!!$!! Invalid Kafka Signature for TTC/Connect Message !!$!! "),
		}
	}

	logger.Infof("!!§!! Kafka Signature of TTC/Connect Message is valid !!§!!")
	responseChannel <- &kafkaValidationResult{
		kafkaOffset: offset,
		err:         nil,
	}
}

// allValidated returns error if some of the validation flags have not been set
// during validation
func (v *TxValidator) allValidated(txsfltr ledgerUtil.TxValidationFlags, block *common.Block) error {
	for id, f := range txsfltr {
		if peer.TxValidationCode(f) == peer.TxValidationCode_NOT_VALIDATED {
			return errors.Errorf("transaction %d in block %d has skipped validation", id, block.Header.Number)
		}
	}

	return nil
}

func markTXIdDuplicates(txids []string, txsfltr ledgerUtil.TxValidationFlags) {
	txidMap := make(map[string]struct{})

	for id, txid := range txids {
		if txid == "" {
			continue
		}

		_, in := txidMap[txid]
		if in {
			logger.Error("Duplicate txid", txid, "found, skipping")
			txsfltr.SetFlag(id, peer.TxValidationCode_DUPLICATE_TXID)
		} else {
			txidMap[txid] = struct{}{}
		}
	}
}

func (v *TxValidator) validateTx(req *blockValidationRequest, additionalKafkaOffset int, results chan<- *blockValidationResult) {
	block := req.block
	d := req.d
	tIdx := req.tIdx
	txID := ""

	if d == nil {
		results <- &blockValidationResult{
			tIdx: tIdx,
		}
		return
	}

	if env, err := utils.GetEnvelopeFromBlock(d); err != nil {
		logger.Warningf("Error getting tx from block: %+v", err)
		results <- &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_INVALID_OTHER_REASON,
		}
		return
	} else if env != nil {
		// validate the transaction: here we check that the transaction
		// is properly formed, properly signed and that the security
		// chain binding proposal to endorsements to tx holds. We do
		// NOT check the validity of endorsements, though. That's a
		// job for VSCC below
		logger.Debugf("[%s] validateTx starts for block %p env %p txn %d", v.ChainID, block, env, tIdx)
		defer logger.Debugf("[%s] validateTx completes for block %p env %p txn %d", v.ChainID, block, env, tIdx)
		var payload *common.Payload
		var err error
		var txResult peer.TxValidationCode
		var txsChaincodeName *sysccprovider.ChaincodeInstance
		var txsUpgradedChaincode *sysccprovider.ChaincodeInstance

		if payload, txResult = validation.ValidateTransaction(env, v.Support.Capabilities()); txResult != peer.TxValidationCode_VALID {
			logger.Errorf("Invalid transaction with index %d", tIdx)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: txResult,
			}
			return
		}

		chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
		if err != nil {
			logger.Warningf("Could not unmarshal channel header, err %s, skipping", err)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_INVALID_OTHER_REASON,
			}
			return
		}

		channel := chdr.ChannelId
		logger.Debugf("Transaction is for channel %s", channel)

		if !v.chainExists(channel) {
			logger.Errorf("Dropping transaction for non-existent channel %s", channel)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_TARGET_CHAIN_NOT_FOUND,
			}
			return
		}

		if common.HeaderType(chdr.Type) == common.HeaderType_ENDORSER_TRANSACTION {

			txID = chdr.TxId

			// Check duplicate transactions
			erroneousResultEntry := v.checkTxIdDupsLedger(tIdx, chdr, v.Support.Ledger())
			if erroneousResultEntry != nil {
				results <- erroneousResultEntry
				return
			}

			// Validate tx with vscc and policy
			logger.Debug("Validating transaction vscc tx validate")
			err, cde := v.Vscc.VSCCValidateTx(tIdx, payload, d, block)
			if err != nil {
				logger.Errorf("VSCCValidateTx for transaction txId = %s returned error: %s", txID, err)
				switch err.(type) {
				case *commonerrors.VSCCExecutionFailureError:
					results <- &blockValidationResult{
						tIdx: tIdx,
						err:  err,
					}
					return
				case *commonerrors.VSCCInfoLookupFailureError:
					results <- &blockValidationResult{
						tIdx: tIdx,
						err:  err,
					}
					return
				default:
					results <- &blockValidationResult{
						tIdx:           tIdx,
						validationCode: cde,
					}
					return
				}
			}

			invokeCC, upgradeCC, err := v.getTxCCInstance(payload)
			if err != nil {
				logger.Errorf("Get chaincode instance from transaction txId = %s returned error: %+v", txID, err)
				results <- &blockValidationResult{
					tIdx:           tIdx,
					validationCode: peer.TxValidationCode_INVALID_OTHER_REASON,
				}
				return
			}
			txsChaincodeName = invokeCC
			if upgradeCC != nil {
				logger.Infof("Find chaincode upgrade transaction for chaincode %s on channel %s with new version %s", upgradeCC.ChaincodeName, upgradeCC.ChainID, upgradeCC.ChaincodeVersion)
				txsUpgradedChaincode = upgradeCC
			}

			// !!! BEGIN MODIFICATION
			kafkaRes := v.verifyKafkaTxMessage(env, results, tIdx+additionalKafkaOffset)
			if kafkaRes != 0 {
				return
			}
			// !!! END MODIFICATION

			// FAB-12971 comment out below block before v1.4 cut. Will uncomment after v1.4.
			/*
				} else if common.HeaderType(chdr.Type) == common.HeaderType_TOKEN_TRANSACTION {

					txID = chdr.TxId
					if !v.Support.Capabilities().FabToken() {
						logger.Errorf("FabToken capability is not enabled. Unsupported transaction type [%s] in block [%d] transaction [%d]",
							common.HeaderType(chdr.Type), block.Header.Number, tIdx)
						results <- &blockValidationResult{
							tIdx:           tIdx,
							validationCode: peer.TxValidationCode_UNSUPPORTED_TX_PAYLOAD,
						}
						return
					}

					// Check if there is a duplicate of such transaction in the ledger and
					// obtain the corresponding result that acknowledges the error type
					erroneousResultEntry := v.checkTxIdDupsLedger(tIdx, chdr, v.Support.Ledger())
					if erroneousResultEntry != nil {
						results <- erroneousResultEntry
						return
					}

					// Set the namespace of the invocation field
					txsChaincodeName = &sysccprovider.ChaincodeInstance{
						ChainID:          channel,
						ChaincodeName:    "Token",
						ChaincodeVersion: ""}
			*/
		} else if common.HeaderType(chdr.Type) == common.HeaderType_CONFIG {
			configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
			if err != nil {
				err = errors.WithMessage(err, "error unmarshalling config which passed initial validity checks")
				logger.Criticalf("%+v", err)
				results <- &blockValidationResult{
					tIdx: tIdx,
					err:  err,
				}
				return
			}

			if err := v.Support.Apply(configEnvelope); err != nil {
				err = errors.WithMessage(err, "error validating config which passed initial validity checks")
				logger.Criticalf("%+v", err)
				results <- &blockValidationResult{
					tIdx: tIdx,
					err:  err,
				}
				return
			}

			// !!! BEGIN MODIFICATION
			kafkaRes := v.verifyKafkaTxMessage(env, results, tIdx)
			if kafkaRes != 0 {
				return
			}
			// !!! END MODIFICATION

			logger.Debugf("config transaction received for chain %s", channel)
		} else {
			logger.Warningf("Unknown transaction type [%s] in block number [%d] transaction index [%d]",
				common.HeaderType(chdr.Type), block.Header.Number, tIdx)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_UNKNOWN_TX_TYPE,
			}
			return
		}

		if _, err := proto.Marshal(env); err != nil {
			logger.Warningf("Cannot marshal transaction: %s", err)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_MARSHAL_TX_ERROR,
			}
			return
		}
		// Succeeded to pass down here, transaction is valid
		results <- &blockValidationResult{
			tIdx:                 tIdx,
			txsChaincodeName:     txsChaincodeName,
			txsUpgradedChaincode: txsUpgradedChaincode,
			validationCode:       peer.TxValidationCode_VALID,
			txid:                 txID,
		}
		return
	} else {
		logger.Warning("Nil tx from block")
		results <- &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_NIL_ENVELOPE,
		}
		return
	}
}

func (v *TxValidator) verifyKafkaTxMessage(env *common.Envelope, results chan<- *blockValidationResult, tIdx int) int {
	logger.Infof("!!§!! Checking Kafka Message Sequence Number !!§!!")
	logger.Infof("!!§!! Expected %d, got %d !!§!!", v.NewKafkaOffset+tIdx, env.KafkaPayload.KafkaOffset)
	if env.KafkaPayload.KafkaOffset != int64(v.NewKafkaOffset+tIdx) {
		logger.Errorf("!!$!! Invalid Kafka Sequence %d: Got %d, expected %d !!$!! ", tIdx, env.KafkaPayload.KafkaOffset, int64(v.NewKafkaOffset+tIdx))
		results <- &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_INVALID_KAFKASEQUENCENUMBER,
		}
		return -1
	}
	logger.Infof("!!§!! Kafka Sequence Number is as expected !!§!!")

	proof := GetProofFromBytes(env.KafkaPayload.KafkaMerkleProofHeader)

	// Rebuild Kafkas Signed Data
	/*
	* In the Following we describe, how we rebuild the signed data:
	*
	* Kafka signs the ConsumerMessages Payload, which is a marshaled KafkaMessage.
	* The Orderer can cast this KafkaMessage to a KafkaMessageRegular which contains the Payload of the marshaled Envelope and other fields.
	* To avoid redundancy, we marshal the sent Envelope (!! without the newly added KafkaPayload !!) to regain the Payload of the KafkaMessageRegular.
	 */
	oldEnv := &common.Envelope{
		Payload:              env.Payload,
		Signature:            env.Signature,
		XXX_NoUnkeyedLiteral: env.XXX_NoUnkeyedLiteral,
		XXX_unrecognized:     env.XXX_unrecognized,
		XXX_sizecache:        env.XXX_sizecache,
	}

	regMessagePayload, _ := proto.Marshal(oldEnv)

	kafkaMessage := &ab.KafkaMessage{
		Type: &ab.KafkaMessage_Regular{
			Regular: &ab.KafkaMessageRegular{
				Payload:              regMessagePayload,
				ConfigSeq:            env.KafkaPayload.KafkaRegularMessage.ConfigSeq,
				Class:                ab.KafkaMessageRegular_Class(env.KafkaPayload.KafkaRegularMessage.Class),
				OriginalOffset:       env.KafkaPayload.KafkaRegularMessage.OriginalOffset,
				XXX_NoUnkeyedLiteral: env.KafkaPayload.KafkaRegularMessage.XXX_NoUnkeyedLiteral,
				XXX_unrecognized:     env.KafkaPayload.KafkaRegularMessage.XXX_unrecognized,
				XXX_sizecache:        env.KafkaPayload.KafkaRegularMessage.XXX_sizecache,
			},
		},
	}

	marshaledData, _ := proto.Marshal(kafkaMessage)

	//the signed data consists of bytesOf(KafkaOffset) + bytesOf(KafkaTimestamp) + marshaledData

	kafkaSignedData := make([]byte, 16)
	binary.BigEndian.PutUint64(kafkaSignedData[0:8], uint64(env.KafkaPayload.KafkaOffset))
	binary.BigEndian.PutUint64(kafkaSignedData[8:16], uint64(env.KafkaPayload.KafkaTimestamp))
	kafkaSignedData = append(kafkaSignedData, marshaledData...)

	//Verify Merkle Proof
	if !proof.VerifyProof(kafkaSignedData) {
		logger.Errorf("!!$!! Invalid Merkle Proof with index %d !!$!! ", tIdx)
		results <- &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_INVALID_KAFKAMERKLEPROOF,
		}
		return -1
	}

	logger.Infof("!!§!! Kafka Proof is valid !!§!!")
	logger.Infof("!!§!! Checking Kafka Signature !!§!!")

	//Verify Signature
	if proof.VerifySignature(env.KafkaPayload.KafkaSignatureHeader) != nil {
		logger.Errorf("!!$!! Invalid Kafka Signature with index %d !!$!!", tIdx)
		results <- &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_INVALID_KAFKASIGNATURE,
		}
		return -1
	}

	logger.Infof("!!§!! Kafka Signature is valid !!§!!")
	return 0
}

// CheckTxIdDupsLedger returns a vlockValidationResult enhanced with the respective
// error codes if and only if there is transaction with the same transaction identifier
// in the ledger or no decision can be made for whether such transaction exists;
// the function returns nil if it has ensured that there is no such duplicate, such
// that its consumer can proceed with the transaction processing
func (v *TxValidator) checkTxIdDupsLedger(tIdx int, chdr *common.ChannelHeader, ldgr ledger.PeerLedger) (errorTuple *blockValidationResult) {

	// Retrieve the transaction identifier of the input header
	txID := chdr.TxId

	// Look for a transaction with the same identifier inside the ledger
	_, err := ldgr.GetTransactionByID(txID)

	// if returned error is nil, it means that there is already a tx in
	// the ledger with the supplied id
	if err == nil {
		logger.Error("Duplicate transaction found, ", txID, ", skipping")
		return &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_DUPLICATE_TXID,
		}
	}

	// if returned error is not of type blkstorage.NotFoundInIndexErr, it means
	// we could not verify whether a tx with the supplied id is in the ledger
	if _, isNotFoundInIndexErrType := err.(ledger.NotFoundInIndexErr); !isNotFoundInIndexErrType {
		logger.Errorf("Ledger failure while attempting to detect duplicate status for "+
			"txid %s, err '%s'. Aborting", txID, err)
		return &blockValidationResult{
			tIdx: tIdx,
			err:  err,
		}
	}

	// it otherwise means that there is no transaction with the same identifier
	// residing in the ledger
	return nil
}

// generateCCKey generates a unique identifier for chaincode in specific channel
func (v *TxValidator) generateCCKey(ccName, chainID string) string {
	return fmt.Sprintf("%s/%s", ccName, chainID)
}

// invalidTXsForUpgradeCC invalid all txs that should be invalided because of chaincode upgrade txs
func (v *TxValidator) invalidTXsForUpgradeCC(txsChaincodeNames map[int]*sysccprovider.ChaincodeInstance, txsUpgradedChaincodes map[int]*sysccprovider.ChaincodeInstance, txsfltr ledgerUtil.TxValidationFlags) {
	if len(txsUpgradedChaincodes) == 0 {
		return
	}

	// Invalid former cc upgrade txs if there're two or more txs upgrade the same cc
	finalValidUpgradeTXs := make(map[string]int)
	upgradedChaincodes := make(map[string]*sysccprovider.ChaincodeInstance)
	for tIdx, cc := range txsUpgradedChaincodes {
		if cc == nil {
			continue
		}
		upgradedCCKey := v.generateCCKey(cc.ChaincodeName, cc.ChainID)

		if finalIdx, exist := finalValidUpgradeTXs[upgradedCCKey]; !exist {
			finalValidUpgradeTXs[upgradedCCKey] = tIdx
			upgradedChaincodes[upgradedCCKey] = cc
		} else if finalIdx < tIdx {
			logger.Infof("Invalid transaction with index %d: chaincode was upgraded by latter tx", finalIdx)
			txsfltr.SetFlag(finalIdx, peer.TxValidationCode_CHAINCODE_VERSION_CONFLICT)

			// record latter cc upgrade tx info
			finalValidUpgradeTXs[upgradedCCKey] = tIdx
			upgradedChaincodes[upgradedCCKey] = cc
		} else {
			logger.Infof("Invalid transaction with index %d: chaincode was upgraded by latter tx", tIdx)
			txsfltr.SetFlag(tIdx, peer.TxValidationCode_CHAINCODE_VERSION_CONFLICT)
		}
	}

	// invalid txs which invoke the upgraded chaincodes
	for tIdx, cc := range txsChaincodeNames {
		if cc == nil {
			continue
		}
		ccKey := v.generateCCKey(cc.ChaincodeName, cc.ChainID)
		if _, exist := upgradedChaincodes[ccKey]; exist {
			if txsfltr.IsValid(tIdx) {
				logger.Infof("Invalid transaction with index %d: chaincode was upgraded in the same block", tIdx)
				txsfltr.SetFlag(tIdx, peer.TxValidationCode_CHAINCODE_VERSION_CONFLICT)
			}
		}
	}
}

func (v *TxValidator) getTxCCInstance(payload *common.Payload) (invokeCCIns, upgradeCCIns *sysccprovider.ChaincodeInstance, err error) {
	// This is duplicated unpacking work, but make test easier.
	chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, nil, err
	}

	// Chain ID
	chainID := chdr.ChannelId // it is guaranteed to be an existing channel by now

	// ChaincodeID
	hdrExt, err := utils.GetChaincodeHeaderExtension(payload.Header)
	if err != nil {
		return nil, nil, err
	}
	invokeCC := hdrExt.ChaincodeId
	invokeIns := &sysccprovider.ChaincodeInstance{ChainID: chainID, ChaincodeName: invokeCC.Name, ChaincodeVersion: invokeCC.Version}

	// Transaction
	tx, err := utils.GetTransaction(payload.Data)
	if err != nil {
		logger.Errorf("GetTransaction failed: %+v", err)
		return invokeIns, nil, nil
	}

	// ChaincodeActionPayload
	cap, err := utils.GetChaincodeActionPayload(tx.Actions[0].Payload)
	if err != nil {
		logger.Errorf("GetChaincodeActionPayload failed: %+v", err)
		return invokeIns, nil, nil
	}

	// ChaincodeProposalPayload
	cpp, err := utils.GetChaincodeProposalPayload(cap.ChaincodeProposalPayload)
	if err != nil {
		logger.Errorf("GetChaincodeProposalPayload failed: %+v", err)
		return invokeIns, nil, nil
	}

	// ChaincodeInvocationSpec
	cis := &peer.ChaincodeInvocationSpec{}
	err = proto.Unmarshal(cpp.Input, cis)
	if err != nil {
		logger.Errorf("GetChaincodeInvokeSpec failed: %+v", err)
		return invokeIns, nil, nil
	}

	if invokeCC.Name == "lscc" {
		if string(cis.ChaincodeSpec.Input.Args[0]) == "upgrade" {
			upgradeIns, err := v.getUpgradeTxInstance(chainID, cis.ChaincodeSpec.Input.Args[2])
			if err != nil {
				return invokeIns, nil, nil
			}
			return invokeIns, upgradeIns, nil
		}
	}

	return invokeIns, nil, nil
}

func (v *TxValidator) getUpgradeTxInstance(chainID string, cdsBytes []byte) (*sysccprovider.ChaincodeInstance, error) {
	cds, err := utils.GetChaincodeDeploymentSpec(cdsBytes, platforms.NewRegistry(&golang.Platform{}))
	if err != nil {
		return nil, err
	}

	return &sysccprovider.ChaincodeInstance{
		ChainID:          chainID,
		ChaincodeName:    cds.ChaincodeSpec.ChaincodeId.Name,
		ChaincodeVersion: cds.ChaincodeSpec.ChaincodeId.Version,
	}, nil
}

type dynamicDeserializer struct {
	support Support
}

func (ds *dynamicDeserializer) DeserializeIdentity(serializedIdentity []byte) (msp.Identity, error) {
	return ds.support.MSPManager().DeserializeIdentity(serializedIdentity)
}

func (ds *dynamicDeserializer) IsWellFormed(identity *mspprotos.SerializedIdentity) error {
	return ds.support.MSPManager().IsWellFormed(identity)
}

type dynamicCapabilities struct {
	support Support
}

func (ds *dynamicCapabilities) ACLs() bool {
	return ds.support.Capabilities().ACLs()
}

func (ds *dynamicCapabilities) CollectionUpgrade() bool {
	return ds.support.Capabilities().CollectionUpgrade()
}

func (ds *dynamicCapabilities) StorePvtDataOfInvalidTx() bool {
	return ds.support.Capabilities().StorePvtDataOfInvalidTx()
}

// FabToken returns true if fabric token function is supported.
func (ds *dynamicCapabilities) FabToken() bool {
	return ds.support.Capabilities().FabToken()
}

func (ds *dynamicCapabilities) ForbidDuplicateTXIdInBlock() bool {
	return ds.support.Capabilities().ForbidDuplicateTXIdInBlock()
}

func (ds *dynamicCapabilities) KeyLevelEndorsement() bool {
	return ds.support.Capabilities().KeyLevelEndorsement()
}

func (ds *dynamicCapabilities) MetadataLifecycle() bool {
	return ds.support.Capabilities().MetadataLifecycle()
}

func (ds *dynamicCapabilities) PrivateChannelData() bool {
	return ds.support.Capabilities().PrivateChannelData()
}

func (ds *dynamicCapabilities) Supported() error {
	return ds.support.Capabilities().Supported()
}

func (ds *dynamicCapabilities) V1_1Validation() bool {
	return ds.support.Capabilities().V1_1Validation()
}

func (ds *dynamicCapabilities) V1_2Validation() bool {
	return ds.support.Capabilities().V1_2Validation()
}

func (ds *dynamicCapabilities) V1_3Validation() bool {
	return ds.support.Capabilities().V1_3Validation()
}
