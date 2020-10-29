// Code generated by protoc-gen-go. DO NOT EDIT.
// source: github.com/hyperledger/fabric/protos/peer/transaction.proto

package peer // import "github.com/hyperledger/fabric/protos/peer"

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import _ "github.com/golang/protobuf/ptypes/timestamp"
import common "github.com/hyperledger/fabric/protos/common"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type TxValidationCode int32

const (
	TxValidationCode_VALID                        TxValidationCode = 0
	TxValidationCode_NIL_ENVELOPE                 TxValidationCode = 1
	TxValidationCode_BAD_PAYLOAD                  TxValidationCode = 2
	TxValidationCode_BAD_COMMON_HEADER            TxValidationCode = 3
	TxValidationCode_BAD_CREATOR_SIGNATURE        TxValidationCode = 4
	TxValidationCode_INVALID_ENDORSER_TRANSACTION TxValidationCode = 5
	TxValidationCode_INVALID_CONFIG_TRANSACTION   TxValidationCode = 6
	TxValidationCode_UNSUPPORTED_TX_PAYLOAD       TxValidationCode = 7
	TxValidationCode_BAD_PROPOSAL_TXID            TxValidationCode = 8
	TxValidationCode_DUPLICATE_TXID               TxValidationCode = 9
	TxValidationCode_ENDORSEMENT_POLICY_FAILURE   TxValidationCode = 10
	TxValidationCode_MVCC_READ_CONFLICT           TxValidationCode = 11
	TxValidationCode_PHANTOM_READ_CONFLICT        TxValidationCode = 12
	TxValidationCode_UNKNOWN_TX_TYPE              TxValidationCode = 13
	TxValidationCode_TARGET_CHAIN_NOT_FOUND       TxValidationCode = 14
	TxValidationCode_MARSHAL_TX_ERROR             TxValidationCode = 15
	TxValidationCode_NIL_TXACTION                 TxValidationCode = 16
	TxValidationCode_EXPIRED_CHAINCODE            TxValidationCode = 17
	TxValidationCode_CHAINCODE_VERSION_CONFLICT   TxValidationCode = 18
	TxValidationCode_BAD_HEADER_EXTENSION         TxValidationCode = 19
	TxValidationCode_BAD_CHANNEL_HEADER           TxValidationCode = 20
	TxValidationCode_BAD_RESPONSE_PAYLOAD         TxValidationCode = 21
	TxValidationCode_BAD_RWSET                    TxValidationCode = 22
	TxValidationCode_ILLEGAL_WRITESET             TxValidationCode = 23
	TxValidationCode_INVALID_WRITESET             TxValidationCode = 24
	TxValidationCode_INVALID_KAFKAMERKLEPROOF     TxValidationCode = 25
	TxValidationCode_INVALID_KAFKASIGNATURE       TxValidationCode = 26
	TxValidationCode_INVALID_KAFKASEQUENCENUMBER  TxValidationCode = 27
	TxValidationCode_NOT_VALIDATED                TxValidationCode = 254
	TxValidationCode_INVALID_OTHER_REASON         TxValidationCode = 255
)

var TxValidationCode_name = map[int32]string{
	0:   "VALID",
	1:   "NIL_ENVELOPE",
	2:   "BAD_PAYLOAD",
	3:   "BAD_COMMON_HEADER",
	4:   "BAD_CREATOR_SIGNATURE",
	5:   "INVALID_ENDORSER_TRANSACTION",
	6:   "INVALID_CONFIG_TRANSACTION",
	7:   "UNSUPPORTED_TX_PAYLOAD",
	8:   "BAD_PROPOSAL_TXID",
	9:   "DUPLICATE_TXID",
	10:  "ENDORSEMENT_POLICY_FAILURE",
	11:  "MVCC_READ_CONFLICT",
	12:  "PHANTOM_READ_CONFLICT",
	13:  "UNKNOWN_TX_TYPE",
	14:  "TARGET_CHAIN_NOT_FOUND",
	15:  "MARSHAL_TX_ERROR",
	16:  "NIL_TXACTION",
	17:  "EXPIRED_CHAINCODE",
	18:  "CHAINCODE_VERSION_CONFLICT",
	19:  "BAD_HEADER_EXTENSION",
	20:  "BAD_CHANNEL_HEADER",
	21:  "BAD_RESPONSE_PAYLOAD",
	22:  "BAD_RWSET",
	23:  "ILLEGAL_WRITESET",
	24:  "INVALID_WRITESET",
	25:  "INVALID_KAFKAMERKLEPROOF",
	26:  "INVALID_KAFKASIGNATURE",
	27:  "INVALID_KAFKASEQUENCENUMBER",
	254: "NOT_VALIDATED",
	255: "INVALID_OTHER_REASON",
}
var TxValidationCode_value = map[string]int32{
	"VALID":                        0,
	"NIL_ENVELOPE":                 1,
	"BAD_PAYLOAD":                  2,
	"BAD_COMMON_HEADER":            3,
	"BAD_CREATOR_SIGNATURE":        4,
	"INVALID_ENDORSER_TRANSACTION": 5,
	"INVALID_CONFIG_TRANSACTION":   6,
	"UNSUPPORTED_TX_PAYLOAD":       7,
	"BAD_PROPOSAL_TXID":            8,
	"DUPLICATE_TXID":               9,
	"ENDORSEMENT_POLICY_FAILURE":   10,
	"MVCC_READ_CONFLICT":           11,
	"PHANTOM_READ_CONFLICT":        12,
	"UNKNOWN_TX_TYPE":              13,
	"TARGET_CHAIN_NOT_FOUND":       14,
	"MARSHAL_TX_ERROR":             15,
	"NIL_TXACTION":                 16,
	"EXPIRED_CHAINCODE":            17,
	"CHAINCODE_VERSION_CONFLICT":   18,
	"BAD_HEADER_EXTENSION":         19,
	"BAD_CHANNEL_HEADER":           20,
	"BAD_RESPONSE_PAYLOAD":         21,
	"BAD_RWSET":                    22,
	"ILLEGAL_WRITESET":             23,
	"INVALID_WRITESET":             24,
	"INVALID_KAFKAMERKLEPROOF":     25,
	"INVALID_KAFKASIGNATURE":       26,
	"INVALID_KAFKASEQUENCENUMBER":  27,
	"NOT_VALIDATED":                254,
	"INVALID_OTHER_REASON":         255,
}

func (x TxValidationCode) String() string {
	return proto.EnumName(TxValidationCode_name, int32(x))
}
func (TxValidationCode) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_transaction_014a2f728cdb9c85, []int{0}
}

// Reserved entries in the key-level metadata map
type MetaDataKeys int32

const (
	MetaDataKeys_VALIDATION_PARAMETER MetaDataKeys = 0
)

var MetaDataKeys_name = map[int32]string{
	0: "VALIDATION_PARAMETER",
}
var MetaDataKeys_value = map[string]int32{
	"VALIDATION_PARAMETER": 0,
}

func (x MetaDataKeys) String() string {
	return proto.EnumName(MetaDataKeys_name, int32(x))
}
func (MetaDataKeys) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_transaction_014a2f728cdb9c85, []int{1}
}

// This message is necessary to facilitate the verification of the signature
// (in the signature field) over the bytes of the transaction (in the
// transactionBytes field).
type SignedTransaction struct {
	// The bytes of the Transaction. NDD
	TransactionBytes []byte `protobuf:"bytes,1,opt,name=transaction_bytes,json=transactionBytes,proto3" json:"transaction_bytes,omitempty"`
	// Signature of the transactionBytes The public key of the signature is in
	// the header field of TransactionAction There might be multiple
	// TransactionAction, so multiple headers, but there should be same
	// transactor identity (cert) in all headers
	Signature            []byte   `protobuf:"bytes,2,opt,name=signature,proto3" json:"signature,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SignedTransaction) Reset()         { *m = SignedTransaction{} }
func (m *SignedTransaction) String() string { return proto.CompactTextString(m) }
func (*SignedTransaction) ProtoMessage()    {}
func (*SignedTransaction) Descriptor() ([]byte, []int) {
	return fileDescriptor_transaction_014a2f728cdb9c85, []int{0}
}
func (m *SignedTransaction) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SignedTransaction.Unmarshal(m, b)
}
func (m *SignedTransaction) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SignedTransaction.Marshal(b, m, deterministic)
}
func (dst *SignedTransaction) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SignedTransaction.Merge(dst, src)
}
func (m *SignedTransaction) XXX_Size() int {
	return xxx_messageInfo_SignedTransaction.Size(m)
}
func (m *SignedTransaction) XXX_DiscardUnknown() {
	xxx_messageInfo_SignedTransaction.DiscardUnknown(m)
}

var xxx_messageInfo_SignedTransaction proto.InternalMessageInfo

func (m *SignedTransaction) GetTransactionBytes() []byte {
	if m != nil {
		return m.TransactionBytes
	}
	return nil
}

func (m *SignedTransaction) GetSignature() []byte {
	if m != nil {
		return m.Signature
	}
	return nil
}

// ProcessedTransaction wraps an Envelope that includes a transaction along with an indication
// of whether the transaction was validated or invalidated by committing peer.
// The use case is that GetTransactionByID API needs to retrieve the transaction Envelope
// from block storage, and return it to a client, and indicate whether the transaction
// was validated or invalidated by committing peer. So that the originally submitted
// transaction Envelope is not modified, the ProcessedTransaction wrapper is returned.
type ProcessedTransaction struct {
	// An Envelope which includes a processed transaction
	TransactionEnvelope *common.Envelope `protobuf:"bytes,1,opt,name=transactionEnvelope,proto3" json:"transactionEnvelope,omitempty"`
	// An indication of whether the transaction was validated or invalidated by committing peer
	ValidationCode       int32    `protobuf:"varint,2,opt,name=validationCode,proto3" json:"validationCode,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ProcessedTransaction) Reset()         { *m = ProcessedTransaction{} }
func (m *ProcessedTransaction) String() string { return proto.CompactTextString(m) }
func (*ProcessedTransaction) ProtoMessage()    {}
func (*ProcessedTransaction) Descriptor() ([]byte, []int) {
	return fileDescriptor_transaction_014a2f728cdb9c85, []int{1}
}
func (m *ProcessedTransaction) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ProcessedTransaction.Unmarshal(m, b)
}
func (m *ProcessedTransaction) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ProcessedTransaction.Marshal(b, m, deterministic)
}
func (dst *ProcessedTransaction) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ProcessedTransaction.Merge(dst, src)
}
func (m *ProcessedTransaction) XXX_Size() int {
	return xxx_messageInfo_ProcessedTransaction.Size(m)
}
func (m *ProcessedTransaction) XXX_DiscardUnknown() {
	xxx_messageInfo_ProcessedTransaction.DiscardUnknown(m)
}

var xxx_messageInfo_ProcessedTransaction proto.InternalMessageInfo

func (m *ProcessedTransaction) GetTransactionEnvelope() *common.Envelope {
	if m != nil {
		return m.TransactionEnvelope
	}
	return nil
}

func (m *ProcessedTransaction) GetValidationCode() int32 {
	if m != nil {
		return m.ValidationCode
	}
	return 0
}

// The transaction to be sent to the ordering service. A transaction contains
// one or more TransactionAction. Each TransactionAction binds a proposal to
// potentially multiple actions. The transaction is atomic meaning that either
// all actions in the transaction will be committed or none will.  Note that
// while a Transaction might include more than one Header, the Header.creator
// field must be the same in each.
// A single client is free to issue a number of independent Proposal, each with
// their header (Header) and request payload (ChaincodeProposalPayload).  Each
// proposal is independently endorsed generating an action
// (ProposalResponsePayload) with one signature per Endorser. Any number of
// independent proposals (and their action) might be included in a transaction
// to ensure that they are treated atomically.
type Transaction struct {
	// The payload is an array of TransactionAction. An array is necessary to
	// accommodate multiple actions per transaction
	Actions              []*TransactionAction `protobuf:"bytes,1,rep,name=actions,proto3" json:"actions,omitempty"`
	XXX_NoUnkeyedLiteral struct{}             `json:"-"`
	XXX_unrecognized     []byte               `json:"-"`
	XXX_sizecache        int32                `json:"-"`
}

func (m *Transaction) Reset()         { *m = Transaction{} }
func (m *Transaction) String() string { return proto.CompactTextString(m) }
func (*Transaction) ProtoMessage()    {}
func (*Transaction) Descriptor() ([]byte, []int) {
	return fileDescriptor_transaction_014a2f728cdb9c85, []int{2}
}
func (m *Transaction) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Transaction.Unmarshal(m, b)
}
func (m *Transaction) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Transaction.Marshal(b, m, deterministic)
}
func (dst *Transaction) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Transaction.Merge(dst, src)
}
func (m *Transaction) XXX_Size() int {
	return xxx_messageInfo_Transaction.Size(m)
}
func (m *Transaction) XXX_DiscardUnknown() {
	xxx_messageInfo_Transaction.DiscardUnknown(m)
}

var xxx_messageInfo_Transaction proto.InternalMessageInfo

func (m *Transaction) GetActions() []*TransactionAction {
	if m != nil {
		return m.Actions
	}
	return nil
}

// TransactionAction binds a proposal to its action.  The type field in the
// header dictates the type of action to be applied to the ledger.
type TransactionAction struct {
	// The header of the proposal action, which is the proposal header
	Header []byte `protobuf:"bytes,1,opt,name=header,proto3" json:"header,omitempty"`
	// The payload of the action as defined by the type in the header For
	// chaincode, it's the bytes of ChaincodeActionPayload
	Payload              []byte   `protobuf:"bytes,2,opt,name=payload,proto3" json:"payload,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *TransactionAction) Reset()         { *m = TransactionAction{} }
func (m *TransactionAction) String() string { return proto.CompactTextString(m) }
func (*TransactionAction) ProtoMessage()    {}
func (*TransactionAction) Descriptor() ([]byte, []int) {
	return fileDescriptor_transaction_014a2f728cdb9c85, []int{3}
}
func (m *TransactionAction) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_TransactionAction.Unmarshal(m, b)
}
func (m *TransactionAction) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_TransactionAction.Marshal(b, m, deterministic)
}
func (dst *TransactionAction) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TransactionAction.Merge(dst, src)
}
func (m *TransactionAction) XXX_Size() int {
	return xxx_messageInfo_TransactionAction.Size(m)
}
func (m *TransactionAction) XXX_DiscardUnknown() {
	xxx_messageInfo_TransactionAction.DiscardUnknown(m)
}

var xxx_messageInfo_TransactionAction proto.InternalMessageInfo

func (m *TransactionAction) GetHeader() []byte {
	if m != nil {
		return m.Header
	}
	return nil
}

func (m *TransactionAction) GetPayload() []byte {
	if m != nil {
		return m.Payload
	}
	return nil
}

// ChaincodeActionPayload is the message to be used for the TransactionAction's
// payload when the Header's type is set to CHAINCODE.  It carries the
// chaincodeProposalPayload and an endorsed action to apply to the ledger.
type ChaincodeActionPayload struct {
	// This field contains the bytes of the ChaincodeProposalPayload message from
	// the original invocation (essentially the arguments) after the application
	// of the visibility function. The main visibility modes are "full" (the
	// entire ChaincodeProposalPayload message is included here), "hash" (only
	// the hash of the ChaincodeProposalPayload message is included) or
	// "nothing".  This field will be used to check the consistency of
	// ProposalResponsePayload.proposalHash.  For the CHAINCODE type,
	// ProposalResponsePayload.proposalHash is supposed to be H(ProposalHeader ||
	// f(ChaincodeProposalPayload)) where f is the visibility function.
	ChaincodeProposalPayload []byte `protobuf:"bytes,1,opt,name=chaincode_proposal_payload,json=chaincodeProposalPayload,proto3" json:"chaincode_proposal_payload,omitempty"`
	// The list of actions to apply to the ledger
	Action               *ChaincodeEndorsedAction `protobuf:"bytes,2,opt,name=action,proto3" json:"action,omitempty"`
	XXX_NoUnkeyedLiteral struct{}                 `json:"-"`
	XXX_unrecognized     []byte                   `json:"-"`
	XXX_sizecache        int32                    `json:"-"`
}

func (m *ChaincodeActionPayload) Reset()         { *m = ChaincodeActionPayload{} }
func (m *ChaincodeActionPayload) String() string { return proto.CompactTextString(m) }
func (*ChaincodeActionPayload) ProtoMessage()    {}
func (*ChaincodeActionPayload) Descriptor() ([]byte, []int) {
	return fileDescriptor_transaction_014a2f728cdb9c85, []int{4}
}
func (m *ChaincodeActionPayload) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ChaincodeActionPayload.Unmarshal(m, b)
}
func (m *ChaincodeActionPayload) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ChaincodeActionPayload.Marshal(b, m, deterministic)
}
func (dst *ChaincodeActionPayload) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ChaincodeActionPayload.Merge(dst, src)
}
func (m *ChaincodeActionPayload) XXX_Size() int {
	return xxx_messageInfo_ChaincodeActionPayload.Size(m)
}
func (m *ChaincodeActionPayload) XXX_DiscardUnknown() {
	xxx_messageInfo_ChaincodeActionPayload.DiscardUnknown(m)
}

var xxx_messageInfo_ChaincodeActionPayload proto.InternalMessageInfo

func (m *ChaincodeActionPayload) GetChaincodeProposalPayload() []byte {
	if m != nil {
		return m.ChaincodeProposalPayload
	}
	return nil
}

func (m *ChaincodeActionPayload) GetAction() *ChaincodeEndorsedAction {
	if m != nil {
		return m.Action
	}
	return nil
}

// ChaincodeEndorsedAction carries information about the endorsement of a
// specific proposal
type ChaincodeEndorsedAction struct {
	// This is the bytes of the ProposalResponsePayload message signed by the
	// endorsers.  Recall that for the CHAINCODE type, the
	// ProposalResponsePayload's extenstion field carries a ChaincodeAction
	ProposalResponsePayload []byte `protobuf:"bytes,1,opt,name=proposal_response_payload,json=proposalResponsePayload,proto3" json:"proposal_response_payload,omitempty"`
	// The endorsement of the proposal, basically the endorser's signature over
	// proposalResponsePayload
	Endorsements         []*Endorsement `protobuf:"bytes,2,rep,name=endorsements,proto3" json:"endorsements,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *ChaincodeEndorsedAction) Reset()         { *m = ChaincodeEndorsedAction{} }
func (m *ChaincodeEndorsedAction) String() string { return proto.CompactTextString(m) }
func (*ChaincodeEndorsedAction) ProtoMessage()    {}
func (*ChaincodeEndorsedAction) Descriptor() ([]byte, []int) {
	return fileDescriptor_transaction_014a2f728cdb9c85, []int{5}
}
func (m *ChaincodeEndorsedAction) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ChaincodeEndorsedAction.Unmarshal(m, b)
}
func (m *ChaincodeEndorsedAction) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ChaincodeEndorsedAction.Marshal(b, m, deterministic)
}
func (dst *ChaincodeEndorsedAction) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ChaincodeEndorsedAction.Merge(dst, src)
}
func (m *ChaincodeEndorsedAction) XXX_Size() int {
	return xxx_messageInfo_ChaincodeEndorsedAction.Size(m)
}
func (m *ChaincodeEndorsedAction) XXX_DiscardUnknown() {
	xxx_messageInfo_ChaincodeEndorsedAction.DiscardUnknown(m)
}

var xxx_messageInfo_ChaincodeEndorsedAction proto.InternalMessageInfo

func (m *ChaincodeEndorsedAction) GetProposalResponsePayload() []byte {
	if m != nil {
		return m.ProposalResponsePayload
	}
	return nil
}

func (m *ChaincodeEndorsedAction) GetEndorsements() []*Endorsement {
	if m != nil {
		return m.Endorsements
	}
	return nil
}

func init() {
	proto.RegisterType((*SignedTransaction)(nil), "protos.SignedTransaction")
	proto.RegisterType((*ProcessedTransaction)(nil), "protos.ProcessedTransaction")
	proto.RegisterType((*Transaction)(nil), "protos.Transaction")
	proto.RegisterType((*TransactionAction)(nil), "protos.TransactionAction")
	proto.RegisterType((*ChaincodeActionPayload)(nil), "protos.ChaincodeActionPayload")
	proto.RegisterType((*ChaincodeEndorsedAction)(nil), "protos.ChaincodeEndorsedAction")
	proto.RegisterEnum("protos.TxValidationCode", TxValidationCode_name, TxValidationCode_value)
	proto.RegisterEnum("protos.MetaDataKeys", MetaDataKeys_name, MetaDataKeys_value)
}

func init() {
	proto.RegisterFile("github.com/hyperledger/fabric/protos/peer/transaction.proto", fileDescriptor_transaction_014a2f728cdb9c85)
}

var fileDescriptor_transaction_014a2f728cdb9c85 = []byte{
	// 928 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x55, 0xcb, 0x6e, 0xe3, 0x36,
	0x14, 0x1d, 0x67, 0x9a, 0xa4, 0xa1, 0xf3, 0x60, 0x98, 0x97, 0xe3, 0x09, 0x9a, 0x81, 0x17, 0x45,
	0x3a, 0x05, 0x6c, 0x20, 0xb3, 0x68, 0xd1, 0x76, 0x43, 0x4b, 0x37, 0xb1, 0x10, 0x99, 0x54, 0x29,
	0x3a, 0x8f, 0x2e, 0x4a, 0x28, 0x36, 0xc7, 0x31, 0x6a, 0x4b, 0x86, 0xa4, 0x0c, 0x9a, 0x6d, 0x3f,
	0xa0, 0xfd, 0xd4, 0xfe, 0x41, 0x5b, 0x50, 0x0f, 0x3f, 0x32, 0x1d, 0x20, 0x1b, 0xdb, 0x3a, 0xe7,
	0xf0, 0xde, 0x73, 0xee, 0x95, 0x25, 0xf4, 0xe3, 0x70, 0x94, 0x3e, 0x3c, 0xde, 0x37, 0xfb, 0xd1,
	0xa4, 0xf5, 0xf0, 0x34, 0xd5, 0xf1, 0x58, 0x0f, 0x86, 0x3a, 0x6e, 0x7d, 0x08, 0xee, 0xe3, 0x51,
	0xbf, 0x35, 0x8d, 0xa3, 0x34, 0x4a, 0x5a, 0x53, 0xad, 0xe3, 0x56, 0x1a, 0x07, 0x61, 0x12, 0xf4,
	0xd3, 0x51, 0x14, 0x36, 0x33, 0x9c, 0xac, 0xe5, 0x74, 0xfd, 0x74, 0x18, 0x45, 0xc3, 0xb1, 0xce,
	0xd5, 0xf7, 0x8f, 0x1f, 0x5a, 0xe9, 0x68, 0xa2, 0x93, 0x34, 0x98, 0x4c, 0x73, 0x61, 0x9d, 0xbe,
	0xbc, 0xcb, 0x34, 0x8e, 0xa6, 0x51, 0x12, 0x8c, 0x55, 0xac, 0x93, 0x69, 0x14, 0x26, 0xba, 0x28,
	0xf1, 0xfd, 0x8b, 0x4a, 0xf4, 0xa3, 0xc9, 0x24, 0x0a, 0x8b, 0xaf, 0xfc, 0x64, 0xe3, 0x57, 0xb4,
	0xeb, 0x8f, 0x86, 0xa1, 0x1e, 0xc8, 0x79, 0x00, 0xf2, 0x2d, 0xda, 0x5d, 0xc8, 0xa3, 0xee, 0x9f,
	0x52, 0x9d, 0xd4, 0x2a, 0x6f, 0x2b, 0x67, 0x9b, 0x02, 0x2f, 0x10, 0x6d, 0x83, 0x93, 0x13, 0xb4,
	0x91, 0x8c, 0x86, 0x61, 0x90, 0x3e, 0xc6, 0xba, 0xb6, 0x92, 0x89, 0xe6, 0x40, 0xe3, 0x8f, 0x0a,
	0xda, 0xf7, 0xe2, 0xa8, 0xaf, 0x93, 0x64, 0xb9, 0x47, 0x1b, 0xed, 0x2d, 0x94, 0x82, 0xf0, 0xa3,
	0x1e, 0x47, 0x53, 0x9d, 0x75, 0xa9, 0x9e, 0xe3, 0x66, 0x61, 0xb2, 0xc4, 0xc5, 0xff, 0x89, 0xc9,
	0xd7, 0x68, 0xfb, 0x63, 0x30, 0x1e, 0x0d, 0x02, 0x83, 0x5a, 0xd1, 0x20, 0xef, 0xbf, 0x2a, 0x9e,
	0xa1, 0x8d, 0x36, 0xaa, 0x2e, 0xb6, 0x7e, 0x8f, 0xd6, 0xf3, 0x5f, 0x26, 0xd4, 0xeb, 0xb3, 0xea,
	0xf9, 0x71, 0x3e, 0x8c, 0xa4, 0xb9, 0xa0, 0xa2, 0xd9, 0xa7, 0x28, 0x95, 0x0d, 0x40, 0xbb, 0x9f,
	0xb0, 0xe4, 0x10, 0xad, 0x3d, 0xe8, 0x60, 0xa0, 0xe3, 0x62, 0x3a, 0xc5, 0x15, 0xa9, 0xa1, 0xf5,
	0x69, 0xf0, 0x34, 0x8e, 0x82, 0x41, 0x31, 0x91, 0xf2, 0xb2, 0xf1, 0x57, 0x05, 0x1d, 0x5a, 0x0f,
	0xc1, 0x28, 0xec, 0x47, 0x03, 0x9d, 0x57, 0xf1, 0x72, 0x8a, 0xfc, 0x84, 0xea, 0xfd, 0x92, 0x51,
	0xb3, 0x4d, 0x97, 0x75, 0xf2, 0x06, 0xb5, 0x99, 0xc2, 0x2b, 0x04, 0xe5, 0xe9, 0xef, 0xd0, 0x5a,
	0x6e, 0x2d, 0xeb, 0x58, 0x3d, 0x3f, 0x2d, 0x33, 0xcd, 0xba, 0x41, 0x38, 0x88, 0xe2, 0x44, 0x0f,
	0x8a, 0x64, 0x85, 0xbc, 0xf1, 0x67, 0x05, 0x1d, 0x7d, 0x46, 0x43, 0x7e, 0x40, 0xc7, 0x9f, 0xdc,
	0x72, 0xcf, 0x1c, 0x1d, 0x95, 0x02, 0x51, 0xf0, 0x73, 0x43, 0x9b, 0x3a, 0xaf, 0x36, 0xd1, 0x61,
	0x9a, 0xd4, 0x56, 0xb2, 0x51, 0xef, 0x95, 0xb6, 0x60, 0xce, 0x89, 0x25, 0xe1, 0xbb, 0xbf, 0x57,
	0x11, 0x96, 0xbf, 0x5f, 0x2f, 0xad, 0x90, 0x6c, 0xa0, 0xd5, 0x6b, 0xea, 0x3a, 0x36, 0x7e, 0x45,
	0x30, 0xda, 0x64, 0x8e, 0xab, 0x80, 0x5d, 0x83, 0xcb, 0x3d, 0xc0, 0x15, 0xb2, 0x83, 0xaa, 0x6d,
	0x6a, 0x2b, 0x8f, 0xde, 0xb9, 0x9c, 0xda, 0x78, 0x85, 0x1c, 0xa0, 0x5d, 0x03, 0x58, 0xbc, 0xdb,
	0xe5, 0x4c, 0x75, 0x80, 0xda, 0x20, 0xf0, 0x6b, 0x72, 0x8c, 0x0e, 0x32, 0x58, 0x00, 0x95, 0x5c,
	0x28, 0xdf, 0xb9, 0x64, 0x54, 0xf6, 0x04, 0xe0, 0x2f, 0xc8, 0x5b, 0x74, 0xe2, 0xb0, 0xac, 0x83,
	0x02, 0x66, 0x73, 0xe1, 0x83, 0x50, 0x52, 0x50, 0xe6, 0x53, 0x4b, 0x3a, 0x9c, 0xe1, 0x55, 0xf2,
	0x15, 0xaa, 0x97, 0x0a, 0x8b, 0xb3, 0x0b, 0xe7, 0x72, 0x89, 0x5f, 0x23, 0x75, 0x74, 0xd8, 0x63,
	0x7e, 0xcf, 0xf3, 0xb8, 0x90, 0x60, 0x2b, 0x79, 0x3b, 0xf3, 0xb3, 0x5e, 0xfa, 0xf1, 0x04, 0xf7,
	0xb8, 0x4f, 0x5d, 0x25, 0x6f, 0x1d, 0x1b, 0x7f, 0x49, 0x08, 0xda, 0xb6, 0x7b, 0x9e, 0xeb, 0x58,
	0x54, 0x42, 0x8e, 0x6d, 0x98, 0x36, 0x85, 0x81, 0x2e, 0x30, 0xa9, 0x3c, 0xee, 0x3a, 0xd6, 0x9d,
	0xba, 0xa0, 0x8e, 0x6b, 0x8c, 0x22, 0x72, 0x88, 0x48, 0xf7, 0xda, 0xb2, 0x94, 0x00, 0x9a, 0x1b,
	0x71, 0x1d, 0x4b, 0xe2, 0xaa, 0xc9, 0xe6, 0x75, 0x28, 0x93, 0xbc, 0xfb, 0x8c, 0xda, 0x24, 0x7b,
	0x68, 0xa7, 0xc7, 0xae, 0x18, 0xbf, 0x61, 0xc6, 0x95, 0xbc, 0xf3, 0x00, 0x6f, 0x19, 0xbb, 0x92,
	0x8a, 0x4b, 0x90, 0xca, 0xea, 0x50, 0x87, 0x29, 0xc6, 0xa5, 0xba, 0xe0, 0x3d, 0x66, 0xe3, 0x6d,
	0xb2, 0x8f, 0x70, 0x97, 0x0a, 0xbf, 0x93, 0x39, 0x55, 0x20, 0x04, 0x17, 0x78, 0xa7, 0x9c, 0xbb,
	0xbc, 0x2d, 0x22, 0x63, 0x13, 0x0b, 0x6e, 0x3d, 0x47, 0x80, 0x9d, 0x17, 0xb1, 0xb8, 0x0d, 0x78,
	0xd7, 0x44, 0x98, 0x5d, 0xaa, 0x6b, 0x10, 0xbe, 0xc3, 0xd9, 0xdc, 0x0f, 0x21, 0x35, 0xb4, 0x6f,
	0xa6, 0x91, 0xaf, 0x45, 0xc1, 0xad, 0x04, 0x66, 0x24, 0x78, 0xcf, 0x84, 0xcb, 0x16, 0xd4, 0xa1,
	0x8c, 0x81, 0x5b, 0x2e, 0x6e, 0xbf, 0x3c, 0x21, 0xc0, 0xf7, 0x38, 0xf3, 0x61, 0x36, 0xd9, 0x03,
	0xb2, 0x85, 0x36, 0x32, 0xe6, 0xc6, 0x07, 0x89, 0x0f, 0x8d, 0x73, 0xc7, 0x75, 0xe1, 0x92, 0xba,
	0xea, 0x46, 0x38, 0x12, 0x0c, 0x7a, 0x94, 0xa1, 0xc5, 0xea, 0x66, 0x68, 0x8d, 0x9c, 0xa0, 0x5a,
	0x89, 0x5e, 0xd1, 0x8b, 0x2b, 0xda, 0x05, 0x71, 0xe5, 0x82, 0x27, 0x38, 0xbf, 0xc0, 0xc7, 0x66,
	0x3e, 0x4b, 0xec, 0xfc, 0x66, 0xa9, 0x93, 0x53, 0xf4, 0x66, 0x99, 0x83, 0x9f, 0x7b, 0xc0, 0x2c,
	0x60, 0xbd, 0x6e, 0x1b, 0x04, 0x7e, 0x43, 0x08, 0xda, 0x32, 0xf3, 0xcc, 0x24, 0x54, 0x82, 0x8d,
	0xff, 0xa9, 0x90, 0x63, 0xb4, 0x5f, 0x1e, 0xe2, 0xb2, 0x03, 0xc2, 0xac, 0xc9, 0xe7, 0x0c, 0xff,
	0x5b, 0x79, 0x77, 0x86, 0x36, 0xbb, 0x3a, 0x0d, 0xec, 0x20, 0x0d, 0xae, 0xf4, 0x53, 0x62, 0xe2,
	0x16, 0x47, 0xcd, 0xe4, 0x3c, 0x2a, 0x68, 0x17, 0x24, 0x08, 0xfc, 0xaa, 0xdd, 0x47, 0x8d, 0x28,
	0x1e, 0x36, 0x17, 0x9e, 0xf1, 0xcd, 0xfc, 0x19, 0x5f, 0xfe, 0xad, 0xcc, 0x6b, 0xa2, 0x4d, 0x16,
	0x9e, 0x54, 0x5e, 0xd0, 0xff, 0x2d, 0x18, 0xea, 0x5f, 0xbe, 0x79, 0xf1, 0x5b, 0xe6, 0x3e, 0x7f,
	0x73, 0xbd, 0xff, 0x2f, 0x00, 0x00, 0xff, 0xff, 0x26, 0xbb, 0x3c, 0x79, 0xff, 0x06, 0x00, 0x00,
}
