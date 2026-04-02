import { LightningElement, api, track } from 'lwc';
import { ShowToastEvent } from 'lightning/platformShowToastEvent';
import { NavigationMixin } from 'lightning/navigation';

export default class PacepalChatbot extends NavigationMixin(LightningElement) {
    @api recordId;
    @api websocketUrl = 'wss://unabasing-nonerroneous-ava.ngrok-free.dev/ws';

    @track isChatOpen = false;
    @track currentMessage = '';
    @track connectionStatus = 'Disconnected';
    @track isSending = false;

    // Popup State
    @track showPopup = false;
    @track popupHeader = '';
    @track popupSections = [];
    @track popupMode = 'default'; // 'default' (table) or 'listV2'
    @track v2OrderId = '';

    get isPopupListV2Open() {
        return this.popupMode === 'listV2';
    }

    get isV2SidebarOpen() {
        return this.isChatOpen && this.showPopup && this.isPopupListV2Open;
    }

    get isDefaultPopupOpen() {
        return this.showPopup && !this.isPopupListV2Open;
    }

    get chatWindowClass() {
        return this.isV2SidebarOpen ? 'chat-window chat-window--extended' : 'chat-window';
    }

    get chatBodyClass() {
        return this.isV2SidebarOpen ? 'chat-body chat-body--split' : 'chat-body';
    }

    // Core Chat State
    @track messages = [];

    // Session persistence
    sessionId = null;

    websocket = null;
    reconnectAttempts = 0;
    maxReconnectAttempts = 5;

    get connectionStatusClass() {
        return this.connectionStatus === 'Connected'
            ? 'status-indicator connected'
            : 'status-indicator disconnected';
    }


    // --- Lifecycle & WebSocket ---

    toggleChat() {
        this.isChatOpen = !this.isChatOpen;
        if (this.isChatOpen) {
            // Initialize session ID on first open if not already set
            if (!this.sessionId) {
                this.sessionId = this.generateSessionId();
                console.log('Generated session ID:', this.sessionId);
            }
            this.connectWebSocket();
        } else {
            // Close any open popups/sidebars
            if (this.showPopup) {
                this.closePopup();
            }
            this.disconnectWebSocket();
        }
    }

    generateSessionId() {
        // Use crypto.randomUUID if available, otherwise fallback
        if (typeof crypto !== 'undefined' && crypto.randomUUID) {
            return crypto.randomUUID();
        }
        // Fallback: generate UUID v4
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
            const r = Math.random() * 16 | 0;
            const v = c === 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    }

    connectWebSocket() {
        try {
            this.connectionStatus = 'Connecting...';
            this.websocket = new WebSocket(this.websocketUrl);

            this.websocket.onopen = () => {
                console.log('WebSocket connected');
                this.connectionStatus = 'Connected';
                this.reconnectAttempts = 0;
                this.addSystemMessage('Connected to Marketing Agent');

                // Send Init Message with RecordId
                if (this.recordId) {
                    console.log('Sending Record ID:', this.recordId);
                    this.websocket.send(JSON.stringify({
                        type: 'connection_init',
                        session_id: this.sessionId,
                        recordId: this.recordId
                    }));
                }
            };

            this.websocket.onmessage = (event) => {
                this.handleWebSocketMessage(event);
            };


            this.websocket.onclose = () => {
                console.log('WebSocket disconnected');
                this.connectionStatus = 'Disconnected';
                this.addSystemMessage('Disconnected from server');

                if (this.isChatOpen && this.reconnectAttempts < this.maxReconnectAttempts) {
                    this.reconnectAttempts++;
                    setTimeout(() => {
                        this.addSystemMessage(`Reconnecting... (Attempt ${this.reconnectAttempts})`);
                        this.connectWebSocket();
                    }, 3000);
                }
            };

            this.websocket.onerror = (error) => {
                console.error('WebSocket error:', error);
                this.connectionStatus = 'Error';
                this.showToast('Connection Error', 'Failed to connect to the server', 'error');
            };

        } catch (error) {
            console.error('Error connecting to WebSocket:', error);
            this.showToast('Connection Error', error.message, 'error');
        }
    }

    disconnectWebSocket() {
        if (this.websocket) {
            this.websocket.close();
            this.websocket = null;
        }
    }

    // --- Message Handling ---

    handleWebSocketMessage(event) {
        try {
            this.removeThinkingIndicator();
            const data = JSON.parse(event.data);
            console.log('Received:', data);

            if (data.type === 'status') {
                this.addSystemMessage(data.message);
            } else if (data.type === 'response') {
                this.isSending = false;
                if (data.success) {
                    // Check for generated email content FIRST
                    if (data.generated_email_content) {
                        this.addEmailMessage(data.generated_email_content);
                    }

                    this.addAgentMessage(data.response, data.created_records, data.salesforce_data);
                } else {
                    this.addErrorMessage(`Error: ${data.error || data.response}`);
                }
            } else if (data.type === 'review_proposal') {
                this.isSending = false;
                this.addReviewProposalMessage(data);
            } else if (data.type === 'confirmation') {
                this.isSending = false;
                this.addConfirmationMessage(data);
            } else if (data.type === 'chain_reset') {
                // 🛑 DISABLE ALL PREVIOUS INTERACTIVE MESSAGES
                console.log('🔗 Chain Reset received. Disabling previous interactions...');
                this.disableAllInteractions();

                if (data.message) {
                    this.addSystemMessage(data.message);
                }
            } else if (data.type === 'pop-up-list view') {
                this.isSending = false;
                this.addPopupListMessage(data);
            } else if (data.type === 'pop-up-list view2') {
                this.isSending = false;
                this.addPopupListV2Message(data);
            } else if (data.type === 'record_selection') {
                this.isSending = false;
                this.addRecordSelectionMessage(data);
            } else if (data.type === 'file_selection') {
                this.isSending = false;
                this.addFileSelectionMessage(data);
            } else if (data.type === 'error') {
                this.isSending = false;
                this.addErrorMessage(`Error: ${data.message}`);
            }

        } catch (error) {
            console.error('Error parsing message:', error);
            this.removeThinkingIndicator();
            this.isSending = false;
        }
    }

    disableAllInteractions() {
        // Create a new array to trigger reactivity
        this.messages = this.messages.map(msg => {
            let updatedMsg = { ...msg };

            // Disable Review Proposal buttons
            if (updatedMsg.isReview) {
                updatedMsg.isProceeded = true; // Treats as if proceeded/done
                updatedMsg.isEditing = false;
                updatedMsg.isAnswered = true; // Generic flag
            }

            // Disable Confirmation buttons
            if (updatedMsg.isConfirmation) {
                updatedMsg.isAnswered = true;
            }

            return updatedMsg;
        });
    }

    addEmailMessage(emailContent) {
        const msgId = Date.now();
        console.log('📧 Adding Email Message:', JSON.stringify(emailContent));

        const messageObj = {
            id: msgId,
            type: 'email',
            class: 'message message-agent email-card-container',
            isEmail: true, // Flag for HTML template
            subject: emailContent.subject || 'No Subject',
            bodyHtml: emailContent.body_html || '',
            bodyText: emailContent.body_text || '',
            tone: emailContent.tone || 'Professional',
            audience: emailContent.suggested_audience || 'General',
            timestamp: new Date().toLocaleTimeString()
        };

        this.messages.push(messageObj);
        this.scrollToBottom();
    }

    // --- Message Array Management ---

    addSystemMessage(text) {
        this.pushMessage({ id: Date.now(), type: 'system', content: text, class: 'message message-system', isText: true });
    }

    addErrorMessage(text) {
        this.pushMessage({ id: Date.now(), type: 'error', content: text, class: 'message message-error', isText: true });
    }

    // addUserMessage(text) {
    //     this.pushMessage({ id: Date.now(), type: 'user', content: text, class: 'message message-user', isText: true });
    // }

    addUserMessage(text) {
        // First, convert plain URLs to clickable links
        let processedText = text;

        // Regex to find URLs that are NOT already in <a> tags
        const urlRegex = /(https?:\/\/[^\s<]+)/g;

        // Check if text already has <a> tags (from rich text)
        if (!text.includes('<a ')) {
            // Convert plain URLs to clickable links
            processedText = text.replace(urlRegex, '<a href="$1" target="_blank">$1</a>');
        }

        // Then add white color styling to all links
        processedText = processedText.replace(
            /<a /g,
            '<a style="color: #ffffff !important; text-decoration: underline !important;" '
        );

        this.pushMessage({
            id: Date.now(),
            type: 'user',
            isUser: true,
            content: processedText,
            class: 'message message-user',
            isText: true
        });
    }

    renderedCallback() {
        // Manually inject HTML for user messages using message ID
        this.messages.forEach((msg) => {
            if (msg.isUser) {
                const div = this.template.querySelector(`.user-message-html[data-msg-id="${msg.id}"]`);
                if (div && !div.hasAttribute('data-rendered')) {
                    div.innerHTML = msg.content;
                    div.setAttribute('data-rendered', 'true');
                }
            }
        });
    }



    addAgentMessage(text, createdRecords, hasData) {
        const msgId = Date.now();
        let content = this.formatMessage(text);

        const messageObj = {
            id: msgId,
            type: 'agent',
            content: content,
            class: 'message message-agent',
            isText: true, // Explicit flag for HTML template
            timestamp: new Date().toLocaleTimeString()
        };

        this.messages.push(messageObj);

        if (hasData && !createdRecords) {
            this.addSystemMessage('✓ Data processed');
        }

        console.log('🔗 Created Records received:', JSON.stringify(createdRecords));
        if (createdRecords) {
            // Async enrichment
            this.enrichMessageWithLinks(msgId, text, createdRecords);
        }

        this.scrollToBottom();
    }

    // --- Helper to update filtered options ---
    updateFilteredOptions(msg) {
        // Robust normalization: ensure string, trim, lowercase
        const usedNames = new Set(
            msg.fields
                .map(f => f.name ? String(f.name).trim().toLowerCase() : '')
                .filter(n => n.length > 0)
        );

        // Debug: Log formatted list for clear verification (not Proxy)
        console.log('🔍 Used Fields List:', JSON.stringify(Array.from(usedNames)));

        const allOptions = (msg.availableFields || []).map(af => ({ label: af.label, value: af.name }));

        // Update fields with per-row options
        msg.fields = msg.fields.map(f => {
            if (f.isCustom) {
                // Determine options for this specific row
                const currentName = f.name ? String(f.name).trim().toLowerCase() : '';

                const rowOptions = allOptions.filter(opt => {
                    const optValue = String(opt.value).trim().toLowerCase();
                    const isUsed = usedNames.has(optValue);
                    const isCurrent = optValue === currentName;

                    // Keep if NOT used (available) OR if it is the current value (don't hide self)
                    return !isUsed || isCurrent;
                });

                return { ...f, rowOptions: rowOptions };
            }
            return f;
        });

        // Update global filteredAvailableFields (legacy, maybe used elsewhere?)
        msg.filteredAvailableFields = (msg.availableFields || []).filter(af =>
            !usedNames.has(String(af.name).trim().toLowerCase())
        );

        return msg;
    }

    addReviewProposalMessage(data) {
        // Special Message Type
        console.log('📦 Review Proposal Data RAW:', JSON.stringify(data));

        const proposal = data.proposal;
        const fields = proposal.fields || [];
        const relatedRecords = proposal.related_records || [];

        // FIX: Access available_fields from proposal object, not root data
        const availFields = proposal.available_fields || [];
        console.log('📋 Available Fields from backend:', availFields.length);

        const msgId = Date.now();
        let msg = {
            id: msgId,
            type: 'review',
            class: 'message message-review', // Blue emphasis class
            content: data.message,

            // Proposal State (contained within message)
            isReview: true,
            isEditing: false, // Start as Read-Only
            objectName: proposal.object,
            contactCount: proposal.contact_count,
            showRelatedRecords: (proposal.contact_count !== null && proposal.contact_count !== undefined),
            fields: fields.map(f => {
                const meta = availFields.find(af => af.name.toLowerCase() === f.name.toLowerCase());
                const isPicklist = meta && meta.type === 'picklist';
                return {
                    ...f,
                    key: f.name + msgId,
                    isPicklist: isPicklist,
                    picklistValues: isPicklist ? meta.picklistValues : []
                };
            }),
            relatedRecords: [], // Will be populated async
            availableFields: availFields,
            filteredAvailableFields: [], // Init
            fieldOptions: availFields.map(af => ({ label: af.label, value: af.name })), // ✅ For Combobox
            timestamp: new Date().toLocaleTimeString()
        };

        // Initial Filter
        msg = this.updateFilteredOptions(msg);

        console.log('✅ Final Message Object:', JSON.stringify(msg));

        this.messages.push(msg);

        // Async Link Generation for Related Records
        if (relatedRecords.length > 0) {
            this.enrichRelatedRecords(msgId, relatedRecords);
        }

        this.scrollToBottom();
    }
    addConfirmationMessage(data) {
        console.log('✅ Adding Confirmation Message:', JSON.stringify(data));
        const msgId = Date.now();
        const msg = {
            id: msgId,
            type: 'confirmation',
            class: 'message message-agent', // Use safe class
            content: data.message,
            isConfirmation: true,
            options: data.options || ['Yes', 'No'],
            timestamp: new Date().toLocaleTimeString()
        };
        this.messages.push(msg);
        this.scrollToBottom();
    }

    handleOptionSelect(event) {
        const value = event.target.dataset.value;
        const msgId = event.target.dataset.id;

        // Find the message and disable buttons
        const msgIndex = this.messages.findIndex(m => m.id == msgId);
        if (msgIndex !== -1) {
            let newMsg = { ...this.messages[msgIndex] };
            newMsg.isAnswered = true; // Disable buttons
            // Force reactivity by creating a new array reference
            this.messages = [...this.messages.slice(0, msgIndex), newMsg, ...this.messages.slice(msgIndex + 1)];
        }

        this.sendCustomMessage(value, value);
    }



    handleSaveTemplate(event) {
        const msgId = event.target.dataset.id;
        const msgIndex = this.messages.findIndex(m => m.id == msgId);

        if (msgIndex !== -1) {
            const newMsg = { ...this.messages[msgIndex] };
            newMsg.isSaved = true; // Disable Save button
            this.messages[msgIndex] = newMsg;
        }

        // Send a message acting as the user asking to save
        this.sendCustomMessage("Save this email template to Brevo.", "Saving template...");
    }
    async enrichRelatedRecords(msgId, records) {
        const processedRecords = [];

        for (const rec of records) {
            let url = '#';
            try {
                url = await this[NavigationMixin.GenerateUrl]({
                    type: 'standard__recordPage',
                    attributes: {
                        recordId: rec.Id,
                        actionName: 'view'
                    }
                });
            } catch (e) { console.error('Link gen failed', e); }

            processedRecords.push({
                id: rec.Id,
                name: rec.Name,
                email: rec.Email,
                url: url
            });
        }

        const msgIndex = this.messages.findIndex(m => m.id === msgId);
        if (msgIndex !== -1) {
            const newMsg = { ...this.messages[msgIndex] };
            newMsg.relatedRecords = processedRecords;
            this.messages[msgIndex] = newMsg;
        }
    }

    pushMessage(msg) {
        this.messages.push({
            ...msg,
            timestamp: new Date().toLocaleTimeString()
        });
        this.scrollToBottom();
    }

    // --- Review Proposal Interactions (Inline) ---

    // handleToggleEdit(event) {
    //     const msgId = event.target.dataset.id;
    //     const msgIndex = this.messages.findIndex(m => m.id == msgId);
    //     if (msgIndex !== -1) {
    //         // Clone to trigger reactivity
    //         const newMsg = { ...this.messages[msgIndex] };
    //         newMsg.isEditing = !newMsg.isEditing; // Toggle
    //         this.messages[msgIndex] = newMsg;
    //     }
    // }

    handleFieldChange(event) {
        const msgId = event.target.dataset.msgid;
        const fieldName = event.target.dataset.name;
        const newVal = event.target.value;

        const msgIndex = this.messages.findIndex(m => m.id == msgId);
        if (msgIndex !== -1) {
            const newMsg = { ...this.messages[msgIndex] };
            newMsg.fields = newMsg.fields.map(f => {
                if (f.name === fieldName) return { ...f, value: newVal };
                return f;
            });
            this.messages[msgIndex] = newMsg;
        }
    }

    // handleProceed(event) {
    //     const msgId = event.target.dataset.id;
    //     const msg = this.messages.find(m => m.id == msgId);
    //     if (!msg) return;

    //     // Construct confirmation logic
    //     let confirmMsg = `Proceed with creating ${msg.objectName}. `;
    //     const updates = [];
    //     msg.fields.forEach(field => {
    //         if (field.value) updates.push(`${field.name}='${field.value}'`);
    //     });
    //     confirmMsg += `Details: ${updates.join(', ')}.`;

    //     this.sendCustomMessage(confirmMsg);

    //     if (msg.isEditing) {
    //         const msgIndex = this.messages.findIndex(m => m.id == msgId);
    //         this.messages[msgIndex] = { ...msg, isEditing: false };
    //     }
    // }

    // --- Helpers ---

    async enrichMessageWithLinks(msgId, text, recordsMap) {
        // Find message
        let msgIndex = this.messages.findIndex(m => m.id === msgId);
        if (msgIndex === -1) return;

        let enrichedText = text;
        let hasUpdates = false;

        for (const [objectApiName, records] of Object.entries(recordsMap)) {
            for (const record of records) {
                try {
                    const url = await this[NavigationMixin.GenerateUrl]({
                        type: 'standard__recordPage',
                        attributes: {
                            recordId: record.Id,
                            objectApiName: objectApiName,
                            actionName: 'view'
                        }
                    });

                    const linkHtml = `<a href="${url}" target="_blank" style="color: #005fb2; text-decoration: underline; font-weight: bold;">${record.Name}</a>`;
                    const escapedName = record.Name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
                    const nameRegex = new RegExp(escapedName, 'gi');

                    if (nameRegex.test(enrichedText)) {
                        enrichedText = enrichedText.replace(nameRegex, linkHtml);
                        hasUpdates = true;
                    } else if (enrichedText.includes(record.Id)) {
                        enrichedText = enrichedText.replace(record.Id, linkHtml);
                        hasUpdates = true;
                    } else {
                        enrichedText += ` <br/>View: ${linkHtml}`;
                        hasUpdates = true;
                    }
                } catch (e) { console.error(e); }
            }
        }

        if (hasUpdates) {
            // Update Array Reactively
            const newMsg = { ...this.messages[msgIndex] };
            newMsg.content = this.formatMessage(enrichedText, true);
            this.messages[msgIndex] = newMsg;
        }
    }

    formatMessage(text, skipMarkdown = false) {
        if (!text) return '';
        let formatted = text;
        if (!skipMarkdown) {
            formatted = formatted.replace(
                /\[([^\]]+)\]\(([^)]+)\)/g,
                '<a href="$2" target="_blank" style="color: #005fb2; text-decoration: underline;">$1</a>'
            );
        }
        return formatted.replace(/\n/g, '<br/>');
    }

    scrollToBottom() {
        // Need to wait for DOM update
        setTimeout(() => {
            const container = this.template.querySelector('.chat-messages');
            if (container) container.scrollTop = container.scrollHeight;
        }, 100);
    }

    // --- Review Mode Handlers ---

    // handleToggleEdit(event) {
    //     const msgId = event.target.dataset.id;
    //     const msgIndex = this.messages.findIndex(m => m.id == msgId);
    //     if (msgIndex !== -1) {
    //         // Clone to trigger reactivity
    //         const newMsg = { ...this.messages[msgIndex] };
    //         newMsg.isEditing = !newMsg.isEditing;
    //         this.messages[msgIndex] = newMsg;
    //     }
    // }
    handleToggleEdit(event) {
        const msgId = event.target.dataset.id;
        const msgIndex = this.messages.findIndex(m => m.id == msgId);
        if (msgIndex !== -1) {
            const msg = this.messages[msgIndex];

            // ✅ Prevent action if already proceeded
            if (msg.isProceeded) {
                return;
            }

            const newMsg = { ...msg };
            newMsg.isEditing = !newMsg.isEditing;

            this.messages = [
                ...this.messages.slice(0, msgIndex),
                newMsg,
                ...this.messages.slice(msgIndex + 1)
            ];
        }
    }

    handleAddField(event) {
        const msgId = event.target.dataset.id;
        const msgIndex = this.messages.findIndex(m => m.id == msgId);
        if (msgIndex === -1) return;

        let newMsg = { ...this.messages[msgIndex] };
        // Create a unique key
        const newKey = 'custom_' + Date.now();

        newMsg.fields = [...newMsg.fields, {
            key: newKey,
            name: '',   // User edits this
            value: '',  // User edits this
            label: 'New Field',
            isCustom: true,
            isPicklist: false,
            picklistValues: []
        }];

        // Re-filter options
        newMsg = this.updateFilteredOptions(newMsg);

        this.messages[msgIndex] = newMsg;

        // Scroll to make sure new field is visible
        this.scrollToBottom();
    }

    handleFieldChange(event) {
        const msgId = event.target.dataset.msgid;
        const fieldKey = event.target.dataset.key; // Stable ID
        const property = event.target.dataset.property; // 'name' or 'value'
        const newVal = event.target.value;

        const msgIndex = this.messages.findIndex(m => m.id == msgId);
        if (msgIndex !== -1) {
            let newMsg = { ...this.messages[msgIndex] };
            const availableFields = newMsg.availableFields || [];

            newMsg.fields = newMsg.fields.map(f => {
                if (f.key === fieldKey) {
                    let updatedField = { ...f };

                    if (property === 'name') {
                        updatedField.name = newVal;

                        // Check if this field name exists in availableFields
                        const meta = availableFields.find(af => af.name === newVal);
                        if (meta) {
                            updatedField.label = meta.label; // ✅ Update Label!

                            if (meta.type === 'picklist') {
                                updatedField.isPicklist = true;
                                updatedField.picklistValues = meta.picklistValues;
                                updatedField.value = ''; // Reset value on field change
                            } else {
                                updatedField.isPicklist = false;
                                updatedField.picklistValues = [];
                            }
                        } else {
                            updatedField.label = newVal; // Fallback
                            updatedField.isPicklist = false;
                        }
                    } else if (property === 'value') {
                        updatedField.value = newVal;
                    }
                    return updatedField;
                }
                return f;
            });

            // Re-filter options on name change
            if (property === 'name') {
                newMsg = this.updateFilteredOptions(newMsg);
            }

            this.messages[msgIndex] = newMsg;
        }
    }

    // handleProceed(event) {
    //     const msgId = event.target.dataset.id;
    //     const msg = this.messages.find(m => m.id == msgId);
    //     if (!msg) return;

    //     // Construct confirmation logic
    //     let confirmMsg = `Proceed with creating ${msg.objectName}. `;
    //     const updates = [];

    //     msg.fields.forEach(field => {
    //         // Only add if value exists. For custom fields, Name must also exist.
    //         if (field.value && field.name) {
    //             updates.push(`${field.name}='${field.value}'`);
    //         }
    //     });

    //     confirmMsg += `Details: ${updates.join(', ')}.`;

    //     // Pass related records context if available
    //     if (msg.relatedRecords && msg.relatedRecords.length > 0) {
    //         const ids = msg.relatedRecords.map(r => r.id).join(', ');
    //         // Explicitly key off "CampaignMember" so backend rule triggers
    //         confirmMsg += ` AND Create CampaignMember records for the following ${msg.relatedRecords.length} found records: [${ids}]`;
    //     }

    //     this.sendCustomMessage(confirmMsg, 'Proceeding with the proposed details...');

    //     // Switch back to Read Only
    //     if (msg.isEditing) {
    //         const msgIndex = this.messages.findIndex(m => m.id == msgId);
    //         this.messages[msgIndex] = { ...msg, isEditing: false };
    //     }
    // }
    handleProceed(event) {
        const msgId = event.target.dataset.id;
        const msgIndex = this.messages.findIndex(m => m.id == msgId);
        if (msgIndex === -1) return;

        const msg = this.messages[msgIndex];

        // Construct confirmation logic
        let confirmMsg = `Proceed with creating ${msg.objectName}. `;
        const updates = [];

        msg.fields.forEach(field => {
            if (field.value && field.name) {
                updates.push(`${field.name}='${field.value}'`);
            }
        });

        confirmMsg += `Details: ${updates.join(', ')}.`;

        if (msg.relatedRecords && msg.relatedRecords.length > 0) {
            const ids = msg.relatedRecords.map(r => r.id).join(', ');
            confirmMsg += ` AND Create CampaignMember records for the following ${msg.relatedRecords.length} found records: [${ids}]`;
        }

        // ✅ FIRST: Mark as proceeded BEFORE sending message
        const updatedMsg = {
            ...msg,
            isProceeded: true,
            isEditing: false
        };

        // ✅ Force array reactivity with splice + assignment
        this.messages = [
            ...this.messages.slice(0, msgIndex),
            updatedMsg,
            ...this.messages.slice(msgIndex + 1)
        ];

        // THEN send the message
        this.sendCustomMessage(confirmMsg, 'Proceeding with the proposed details...');
    }

    // --- Input Handling ---
    handleSaveTemplate(event) {
        const msgId = event.target.dataset.id;
        const msgIndex = this.messages.findIndex(m => m.id == msgId);

        if (msgIndex !== -1) {
            let newMsg = { ...this.messages[msgIndex] };
            newMsg.isSaved = true; // Disable Save button
            // Force reactivity by creating a new array reference
            this.messages = [...this.messages.slice(0, msgIndex), newMsg, ...this.messages.slice(msgIndex + 1)];
        }

        // Send a message acting as the user asking to save
        this.sendCustomMessage("Save this email template to Brevo.", "Saving template...");
    }
    handleMessageChange(event) { this.currentMessage = event.target.value; }

    handleKeyPress(event) {
        if (event.key === 'Enter' && !event.shiftKey) {
            event.preventDefault();
            this.sendMessage();
        }
    }

    sendMessage(uiLabel = null) {
        if (!this.currentMessage.trim()) return;
        if (!this.websocket || this.websocket.readyState !== WebSocket.OPEN) {
            this.showToast('Not Connected', 'Wait for connection', 'warning');
            return;
        }

        // Fix: onclick passes an Event object as first arg. Ensure uiLabel is a string or null.
        let displayLabel = null;
        if (typeof uiLabel === 'string') {
            displayLabel = uiLabel;
        }

        // Use uiLabel if provided (cleaner UI), otherwise show the full message
        this.addUserMessage(displayLabel || this.currentMessage);

        try {
            this.websocket.send(JSON.stringify({
                message: this.currentMessage,
                session_id: this.sessionId
            }));
            this.isSending = true;
            this.addThinkingIndicator();
            this.currentMessage = '';
        } catch (error) {
            this.isSending = false;
            this.removeThinkingIndicator();
        }
    }

    sendCustomMessage(msg, uiLabel = null) {
        this.currentMessage = msg;
        this.sendMessage(uiLabel);
    }

    // --- Thinking Indicator (Now just a special message?) ---
    // Actually simpler to just have a boolean isThinking and render it in HTML 
    // BUT to keep message structure, let's just append a temporary message

    addThinkingIndicator() {
        this.pushMessage({
            id: 'thinking',
            type: 'thinking',
            content: '',
            class: 'message message-agent thinking-message',
            isThinking: true
        });
    }

    removeThinkingIndicator() {
        this.messages = this.messages.filter(m => m.type !== 'thinking');
    }

    // --- NEW: Record Selection Logic ---

    async addRecordSelectionMessage(data) {
        const msgId = Date.now();
        const records = data.records || []; // Expecting array of {Id, Name, ...}

        // 1. Process URLs asynchronously
        const processedRecords = await Promise.all(records.map(async (rec) => {
            let url = '#';
            try {
                url = await this[NavigationMixin.GenerateUrl]({
                    type: 'standard__recordPage',
                    attributes: {
                        recordId: rec.Id,
                        actionName: 'view'
                    }
                });
            } catch (e) {
                console.error('URL Gen Error', e);
            }

            return {
                ...rec,
                url: url,
                displayLabel: rec.Name || rec.Subject || rec.CaseNumber || 'Record'
            };
        }));

        // 2. Create Message Object
        const msg = {
            id: msgId,
            type: 'record_selection',
            class: 'message message-agent',
            content: data.message || 'Please select a record:',
            isRecordSelection: true,
            hasSelected: false, // Controls visibility of buttons
            records: processedRecords,
            timestamp: new Date().toLocaleTimeString()
        };

        this.messages.push(msg);
        this.scrollToBottom();
    }

    handleRecordSelect(event) {
        const msgId = event.target.dataset.msgid;
        const recId = event.target.dataset.recid;
        const recName = event.target.dataset.recname;

        // 1. Find message index
        const msgIndex = this.messages.findIndex(m => m.id == msgId);
        if (msgIndex === -1) return;

        // 2. Send selection to server
        this.sendCustomMessage(recId, `Selected: ${recName}`);

        // 3. Update Message State to Remove Buttons
        const updatedMsg = { ...this.messages[msgIndex] };
        updatedMsg.hasSelected = true;

        // Optional: Update content to reflect selection visually in the old message
        updatedMsg.content = `${updatedMsg.content} <br/><b>Selected: ${recName}</b>`;

        this.messages[msgIndex] = updatedMsg;
    }

    addFileSelectionMessage(data) {
        const msgId = Date.now();
        const files = data.data || [];

        const msg = {
            id: msgId,
            type: 'file_selection',
            class: 'message message-agent',
            content: data.response || 'Please select a file:',
            isFileSelection: true,
            files: files.map(f => ({
                id: f.ContentDocumentId,
                name: f.ContentDocument.Title,
                extension: f.ContentDocument.FileExtension,
                label: `${f.ContentDocument.Title}.${f.ContentDocument.FileExtension}`
            })),
            timestamp: new Date().toLocaleTimeString()
        };

        this.messages.push(msg);
        this.scrollToBottom();
    }

    handleFileSelect(event) {
        const fileId = event.target.dataset.id;
        const fileName = event.target.dataset.name;
        this.sendCustomMessage(fileId, `Selected: ${fileName}`);
    }

    // --- NEW: Pop-up List View 2 Logic ---

    addPopupListV2Message(data) {
        const msgId = Date.now();
        const msg = {
            id: msgId,
            type: 'popup_list_v2',
            class: 'message message-agent',
            content: data.message || 'Click here to open details',
            isPopupListV2: true,
            orderId: data.orderId || '',
            timestamp: new Date().toLocaleTimeString()
        };
        this.messages.push(msg);
        this.scrollToBottom();
    }

    handleOpenPopupV2(event) {
        const msgId = event.target.dataset.id;
        const msg = this.messages.find(m => m.id == msgId);
        if (!msg) return;

        this.v2OrderId = msg.orderId || '';
        this.popupMode = 'listV2';
        this.showPopup = true;
    }

    // --- NEW: Pop-up List View Logic ---

    addPopupListMessage(data) {
        const msgId = Date.now();
        const msg = {
            id: msgId,
            type: 'popup_list',
            class: 'message message-agent',
            content: data.message || 'Click here to open details',
            isPopupList: true,
            popupHeader: data['pop-header'] || 'Details',
            categories: data.categories || null,
            records: data.records || [],
            timestamp: new Date().toLocaleTimeString()
        };
        this.messages.push(msg);
        this.scrollToBottom();
    }

    async handleOpenPopup(event) {
        const msgId = event.target.dataset.id;
        const msg = this.messages.find(m => m.id == msgId);
        if (msg) {
            this.popupHeader = msg.popupHeader;
            this.popupMode = 'default'; // Default table mode
            this.popupSections = [];

            let sectionsToProcess = [];

            if (msg.categories) {
                // Handle Categorized Data
                Object.entries(msg.categories).forEach(([category, records]) => {
                    if (records && records.length > 0) {
                        sectionsToProcess.push({
                            id: category,
                            title: category,
                            class: `popup-section-header category-${category.toLowerCase().replace(/\s+/g, '-')}`,
                            items: records
                        });
                    }
                });
            } else {
                // Handle Flat List (Backward Compatibility)
                if (msg.records && msg.records.length > 0) {
                    sectionsToProcess.push({
                        id: 'default',
                        title: '', // No header for flat list
                        class: 'popup-section-header hidden',
                        items: msg.records
                    });
                }
            }

            // Process sections and generate Table Data
            for (const section of sectionsToProcess) {
                // 1. Determine Columns dynamically
                const allKeys = new Set();
                section.items.forEach(rec => Object.keys(rec).forEach(k => allKeys.add(k)));

                // Filter out internal fields if needed (e.g., 'attributes' in SF data)
                const columns = Array.from(allKeys)
                    .filter(k => k !== 'attributes')
                    .map(k => ({ label: k, fieldName: k }));

                // 2. Process Rows
                const rows = await Promise.all(section.items.map(async (rec, index) => {
                    const cells = await Promise.all(columns.map(async (col) => {
                        const key = col.fieldName;
                        const value = rec[key];
                        let isLink = false;
                        let url = '#';

                        // Check if field is an ID
                        if (value && (typeof value === 'string') && (key.toLowerCase() === 'id' || key.toLowerCase().endsWith('id'))) {
                            if (value.length === 15 || value.length === 18) {
                                try {
                                    url = await this[NavigationMixin.GenerateUrl]({
                                        type: 'standard__recordPage',
                                        attributes: {
                                            recordId: value,
                                            actionName: 'view'
                                        }
                                    });
                                    isLink = true;
                                } catch (e) {
                                    console.error('URL Gen Error', e);
                                }
                            }
                        }

                        return { key, value, isLink, url };
                    }));

                    return {
                        id: `${section.id}-${index}`,
                        cells: cells
                    };
                }));

                this.popupSections.push({
                    ...section,
                    columns: columns,
                    rows: rows
                });
            }

            this.showPopup = true;
        }
    }

    closePopup() {
        this.showPopup = false;
        this.popupSections = [];
        this.popupHeader = '';
        this.popupMode = 'default';
        this.v2OrderId = '';
    }

    showToast(title, message, variant) {

        this.dispatchEvent(new ShowToastEvent({ title, message, variant }));
    }

    disconnectedCallback() {
        this.disconnectWebSocket();
    }
}