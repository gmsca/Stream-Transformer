import { HttpClient, HttpErrorResponse, HttpResponse } from '@angular/common/http';
import { Component, OnDestroy, OnInit } from '@angular/core';
import { FormBuilder, FormGroup } from '@angular/forms';
import { RxStompService } from '@stomp/ng2-stompjs';
import { Message } from '@stomp/stompjs';
import { Observable, of, Subject } from 'rxjs';
import { catchError, takeUntil } from 'rxjs/operators';

@Component({
  selector: 'app-message-stream',
  templateUrl: './message-stream.component.html',
  styleUrls: ['./message-stream.component.css']
})

export class MessageStreamComponent implements OnInit, OnDestroy {

  myForm: FormGroup;

  private _messages: Array<string> = [];
  // private _parsedMsg: string;
  claims: Array<Claim> = [];

  get messages() { return this._messages; }
  set messages(value: any) { this._messages = value; }

  // get parsedMsg(): string { return this._parsedMsg; }
  // set parsedMsg(value: string) { this._parsedMsg = value; }

  private destroy$ = new Subject();

  processMessages() {
    let claim = new Claim();

    this.messages.forEach(msg => {
      let parseMsg = JSON.parse(msg);
      // put in claim class
      claim.id = parseMsg['CL_ClaimID'];
      claim.isSubmitted = parseMsg['CL_FeeSubmitted'];
      claim.totalOwed = parseMsg['CL_TotalOwed'];
      claim.isPaid = parseMsg['CL_Paid'];
      claim.csDescription = parseMsg['CS_Description'];
      claim.processDate = parseMsg['CL_ProcessDate'];
      claim.cbDescription = parseMsg['CB_Description'];

      this.claims.push(claim);
      this.claims = this.claims.slice(-1);
    });

    console.log(claim);
  }

  constructor(private frmBuilder: FormBuilder,
    private http: HttpClient,
    private rxStompService: RxStompService) {

    this.myForm = frmBuilder.group(
      { nMessage: '10' }
    );
  }

  ngOnInit(): void {
    if (this.rxStompService.connected) {
      this.rxStompService.watch('/topic/messages')
        .pipe(
          takeUntil(this.destroy$)
        ).subscribe((message: Message) => {
          console.log('Received from websocket: ' + message.body);
          this.messages.push(message.body);
          this.messages = this.messages.slice(-1);
          this.processMessages();
        });
    }
    else {
      console.log("rxStmpeService not connected.");
    };
  }

  ngOnDestroy(): void {
    this.destroy$.next(null);
    this.destroy$.unsubscribe();
  }

  submit(): void {
    const nMessage = this.myForm.controls.nMessage.value;

    this.http.get(`/api/kafka/sample/${nMessage}`, { observe: 'response' })
      .pipe(
        catchError(this.handleError.bind(this)),
        takeUntil(this.destroy$)
      ).subscribe((resp: HttpResponse<any>) => {

      });
  }

  private handleError(error: HttpErrorResponse): Observable<any> {
    return of(null);
  }
}

class Claim {
  id: number;                // CL_CLaimID
  isSubmitted: boolean;      // CL_FeeSubmitted
  totalOwed: number;         // CL_TotalOwed   
  isPaid: boolean;           // CL_Paid
  csDescription: String;     // CS_Description
  processDate: Date;         // CL_ProcessDate
  cbDescription: String;     // CB_Description
}