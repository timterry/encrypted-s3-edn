(ns tterry/encrypted-s3-edn.core
  (:require [clojure.edn :as edn]
            [clojure.tools.logging :as logging])
  (:import (com.amazonaws.regions Regions)
           (com.amazonaws.auth DefaultAWSCredentialsProviderChain)
           (java.io ByteArrayInputStream InputStreamReader PushbackReader)
           (com.amazonaws.services.s3.model PutObjectRequest ObjectMetadata CryptoConfiguration KMSEncryptionMaterialsProvider EncryptedGetObjectRequest)
           (com.amazonaws.services.s3 AmazonS3EncryptionClient)))

(defn- content->input-stream[content]
  (let [str (with-out-str (clojure.pprint/pprint content))]
    (ByteArrayInputStream. (.getBytes str))))

(defn encrypt-and-upload-as-edn [^String kms-region-name ^String kms-key-id ^String s3-bucket ^String s3-key content]
  (logging/info (format "Uploading and encrypting content to s3 at %s/%s using kms key %s in %s"
                        s3-bucket s3-key kms-key-id kms-region-name))
  (let [region (Regions/fromName kms-region-name)
        kms-materials-provider (KMSEncryptionMaterialsProvider. kms-key-id)
        encryption-client  (AmazonS3EncryptionClient. (.getCredentials (DefaultAWSCredentialsProviderChain.))
                                                      kms-materials-provider
                                                      (.withKmsRegion (CryptoConfiguration.) region))
        put-object-request (PutObjectRequest. s3-bucket
                                              s3-key
                                              (content->input-stream content)
                                              (ObjectMetadata.))]
    (.putObject encryption-client put-object-request)))

(defn- download-and-decrypt-s3-file [^String kms-region-name ^String kms-key-id ^String s3-bucket ^String s3-key]
  (let [region (Regions/fromName kms-region-name)
        get-object-request (EncryptedGetObjectRequest. s3-bucket s3-key)
        kms-materials-provider (KMSEncryptionMaterialsProvider. kms-key-id)
        encryption-client (AmazonS3EncryptionClient. (.getCredentials (DefaultAWSCredentialsProviderChain.))
                                                      kms-materials-provider
                                                      (.withKmsRegion (CryptoConfiguration.) region))]
    (.getObjectContent (.getObject encryption-client get-object-request))))

(defn- inputstream->edn [inputstream]
  (with-open [pbr (PushbackReader. (InputStreamReader. inputstream))]
    (edn/read pbr)))

(defn retrieve-and-decrypt-edn [^String kms-region-name ^String kms-key-id ^String s3-bucket ^String s3-key]
  (logging/info (format "Downloading and decrypting s3 file from %s/%s using kms key %s in %s"
                        s3-bucket s3-key kms-key-id kms-region-name))
  (let [s3-input-stream (download-and-decrypt-s3-file kms-region-name kms-key-id s3-bucket s3-key)]
    (inputstream->edn s3-input-stream)))