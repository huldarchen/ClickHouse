//
// HTTPServerSession.h
//
// Library: Net
// Package: HTTPServer
// Module:  HTTPServerSession
//
// Definition of the HTTPServerSession class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPServerSession_INCLUDED
#define Net_HTTPServerSession_INCLUDED


#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTTPServerSession.h"
#include "Poco/Net/HTTPSession.h"
#include "Poco/Net/Net.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Timespan.h"


namespace Poco
{
namespace Net
{


    class Net_API HTTPServerSession : public HTTPSession
    /// This class handles the server side of a
    /// HTTP session. It is used internally by
    /// HTTPServer.
    {
    public:
        HTTPServerSession(const StreamSocket & socket, HTTPServerParams::Ptr pParams);
        /// Creates the HTTPServerSession.

        virtual ~HTTPServerSession();
        /// Destroys the HTTPServerSession.

        bool hasMoreRequests();
        /// Returns true if there are requests available.

        bool canKeepAlive() const;
        /// Returns true if the session can be kept alive.

        SocketAddress clientAddress();
        /// Returns the client's address.

        SocketAddress serverAddress();
        /// Returns the server's address.

        void setKeepAliveTimeout(Poco::Timespan keepAliveTimeout);

        size_t getKeepAliveTimeout() const { return _keepAliveTimeout.totalSeconds(); }

        size_t getMaxKeepAliveRequests() const { return _maxKeepAliveRequests; }

    private:
        bool _firstRequest;
        Poco::Timespan _keepAliveTimeout;
        size_t _maxKeepAliveRequests;
    };


    //
    // inlines
    //
    inline bool HTTPServerSession::canKeepAlive() const
    {
        return getKeepAlive() && _maxKeepAliveRequests > 0;
    }


}
} // namespace Poco::Net


#endif // Net_HTTPServerSession_INCLUDED
