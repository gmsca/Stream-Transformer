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
  parsedVal: any;
  private _values = [];

  get messages() { return this._messages; }
  set messages(value: any) {
    // console.log("bbb", value);
    this.parsedVal = JSON.parse(value);

    for (const [key, value] of Object.entries(this.parsedVal)) {
      console.log(`${key}: ${value}`);
    }
  }

  get values() { return this._values; }
  set values(value) { this._values = value; }

  private destroy$ = new Subject();

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
          this.messages = this.messages.slice(-3);
        });
    }
    else {
      console.log("rxStmpeService not connected");
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

// class Claim {
//   id: number;                // CL_CLaimID
//   isSubmitted: boolean;      // CL_FeeSubmitted
//   totalOwed: number;         // CL_TotalOwed   
//   isPaid: boolean;           // CL_Paid
//   csDescription: String;     // CS_Description
//   processDate: Date;         // CL_ProcessDate
//   cbDescription: String;     // CB_Description
// }